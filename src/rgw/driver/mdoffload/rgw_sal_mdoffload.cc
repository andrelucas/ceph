/**
 * @file rgw_sal_mdoffload.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief SAL Metadata Offload Driver - Implementation
 * @version 0.1
 * @date 2025-09-23
 *
 * @copyright Copyright (c) 2025
 *
 */

#include "rgw_sal_mdoffload.h"

#include <set>

#include "common/dout.h"
#include "global/global_context.h"
#include "mdoffload/v1/mdoffload.pb.h"
#include "rgw_common.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

// Shorthand to check if we are configured at runtime to emit logs at a
// certain level. Used to avoid building log message strings unnecessarily.
#define LOG_ENABLED(cct, level) ((cct)->_conf->subsys.should_gather(dout_subsys, (level)))
// Variant taking a dpp instead of a cct.
#define LOG_ENABLED_D(dpp, level) LOG_ENABLED((dpp)->get_cct(), level)
// Global version.
#define LOG_ENABLED_G(level) LOG_ENABLED(g_ceph_context, level)

#define COND_LOG(cct, level, msg, ...)                                         \
  do {                                                                         \
    if (LOG_ENABLED(cct, level)) {                                             \
      ldout(cct, level) << fmt::format(FMT_STRING(msg), __VA_ARGS__) << dendl; \
    }                                                                          \
  } while (0)
#define COND_LOG_D(dpp, level, msg, ...)                                           \
  do {                                                                             \
    if (LOG_ENABLED_D(dpp, level)) {                                               \
      ldpp_dout(dpp, level) << fmt::format(FMT_STRING(msg), __VA_ARGS__) << dendl; \
    }                                                                              \
  } while (0)
#define COND_LOG_G(level, msg, ...)                                                       \
  do {                                                                                    \
    if (LOG_ENABLED_G(level)) {                                                           \
      ldout(g_ceph_context, level) << fmt::format(FMT_STRING(msg), __VA_ARGS__) << dendl; \
    }                                                                                     \
  } while (0)

namespace akamai::grpcutil {
rgw::sal::Attrs attrs_from_proto(const ::google::protobuf::Map<std::string, std::string>& proto_attrs)
{
  rgw::sal::Attrs attrs;
  for (const auto& kv : proto_attrs) {
    bufferlist bl;
    bl.append(kv.second);
    attrs.emplace(kv.first, std::move(bl));
  }
  return attrs;
}

}
namespace rgw::sal {

/****************************************************************************/

/**
 * @brief Get the next user in the filter chain.
 *
 * Copied from rgw_sal_filter.cc. We do have to override User because we need
 * to deal with User::create_bucket(), where bucket attributes get stored.*
 * @param t
 * @return User*
 */
static inline User* nextUser(User* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<MDOffloadUser*>(t)->get_next();
}

static inline Object* nextObject(Object* t)
{
  if (!t)
    return nullptr;

  if (auto* mo = dynamic_cast<MDOffloadObject*>(t)) {
    return mo->get_next();
  }
  return t;
}

/****************************************************************************/

// MDOffloadFilterDriver

int MDOffloadFilterDriver::initialize(CephContext* cct, const DoutPrefixProvider* dpp)
{
  // MUST call base class initialize().
  int ret = FilterDriver::initialize(cct, dpp);
  if (ret < 0)
    return ret;

  // XXX no mTLS, no channel parameters, no nothing.
  auto uri = cct->_conf->rgw_mdoffload_grpc_uri;
  if (uri.empty()) {
    ldpp_dout(dpp, 0) << "MDOffloadFilterDriver::initialize: no gRPC URI configured" << dendl;
    return -1;
  }
  channelwrapper_ = std::make_shared<gutil::GrpcChannelWrapper>(uri);

  return 0;
}

const std::string MDOffloadFilterDriver::get_name() const
{
  std::string name = "mdoffload<" + next->get_name() + ">";
  return name;
}

std::unique_ptr<User> MDOffloadFilterDriver::get_user(const rgw_user& u)
{
  std::unique_ptr<User> user = next->get_user(u);
  return std::make_unique<MDOffloadUser>(std::move(user), this);
}

int MDOffloadFilterDriver::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_access_key(dpp, key, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new MDOffloadUser(std::move(nu), this);
  user->reset(u);
  return 0;
}

int MDOffloadFilterDriver::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_email(dpp, email, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new MDOffloadUser(std::move(nu), this);
  user->reset(u);
  return 0;
}

int MDOffloadFilterDriver::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_swift(dpp, user_str, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new MDOffloadUser(std::move(nu), this);
  user->reset(u);
  return 0;
}

std::unique_ptr<Object> MDOffloadFilterDriver::get_object(const rgw_obj_key& k)
{
  // Called from RGWHandler_REST_S3::init_from_header() to create an object with no
  // bucket reference.
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadFilterDriver({})::get_object: rgw_obj_key k={}"), (void*)this, k)
      << dendl;
  std::unique_ptr<Object> o = next->get_object(k);
  if (!o) {
    // This really shouldn't happen, but log it if it does.
    ldout(g_ceph_context, 0)
        << fmt::format(FMT_STRING("MDOffloadFilterDriver::get_object: next->get_object() failed for key {}"), k)
        << dendl;
    return nullptr;
  }
  return std::make_unique<MDOffloadObject>(std::move(o), this);
}

// get_bucket() type 1.
int MDOffloadFilterDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadFilterDriver::get_bucket: (variant 1) rgw_bucket b={} nu={}"), b,
             fmt_maybe(nu))
      << dendl;

  ret = next->get_bucket(dpp, nu, b, &nb, y);
  if (ret != 0)
    return ret;

  // Bucket exists. Need to preload the bucket attributes.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadFilterDriver::get_bucket: (variant 3) fetched bucket name={} id={}"),
             nb->get_name(), nb->get_bucket_id())
      << dendl;

  // Fetch attributes for this bucket.
  auto client = channel()->create_client<gutil::MDOffloadGrpcClient>();

  ::grpc::ClientContext context;
  mdoffload::v1::GetBucketAttributesRequest request;
  mdoffload::v1::GetBucketAttributesResponse response;

  request.set_user_id(u->get_display_name());
  request.set_bucket_name(nb->get_name());
  request.set_bucket_id(nb->get_bucket_id());

  auto status = client->stub()->GetBucketAttributes(&context, request, &response);
  if (!status.ok()) {
    ldpp_dout(dpp, 0)
        << fmt::format(FMT_STRING("MDOffloadFilterDriver::get_bucket: (variant 3) gRPC GetBucketAttributes failed: {}"), status.error_message())
        << dendl;
    return -1;
  }

  Bucket* fb = new MDOffloadBucket(std::move(nb), u, this);
  fb->set_attrs(::akamai::grpcutil::attrs_from_proto(response.attributes()));
  bucket->reset(fb);
  return 0;
}

// get_bucket() type 2,
int MDOffloadFilterDriver::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadFilterDriver::get_bucket: (variant 2) RGWBucketInfo i={} nu={}"), i,
             fmt_maybe(nu))
      << dendl;

  ret = next->get_bucket(nu, i, &nb);
  if (ret != 0)
    return ret;

  // Bucket exists. Need to preload the bucket attributes.
  // XXX

  Bucket* fb = new MDOffloadBucket(std::move(nb), u, this);
  bucket->reset(fb);
  return 0;
}

// get_bucket() type 3. Called by create_bucket().
int MDOffloadFilterDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  User* nu = nextUser(u);
  int ret;

  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadFilterDriver::get_bucket: (variant 3) tenant={} name={} nu={}"), tenant, name,
             fmt_maybe(nu))
      << dendl;

  ret = next->get_bucket(dpp, nu, tenant, name, &nb, y);
  if (ret != 0)
    return ret;

  // Bucket exists. Need to preload the bucket attributes.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadFilterDriver::get_bucket: (variant 3) fetched bucket name={} id={}"),
             nb->get_name(), nb->get_bucket_id())
      << dendl;

  // Fetch attributes for this bucket.
  auto client = channel()->create_client<gutil::MDOffloadGrpcClient>();

  ::grpc::ClientContext context;
  mdoffload::v1::GetBucketAttributesRequest request;
  mdoffload::v1::GetBucketAttributesResponse response;

  request.set_user_id(u->get_display_name());
  request.set_bucket_name(name);
  request.set_bucket_id(nb->get_bucket_id());

  auto status = client->stub()->GetBucketAttributes(&context, request, &response);
  if (!status.ok()) {
    ldpp_dout(dpp, 0)
        << fmt::format(FMT_STRING("MDOffloadFilterDriver::get_bucket: (variant 3) gRPC GetBucketAttributes failed: {}"), status.error_message())
        << dendl;
    return -1;
  }
  // We'll load the attributes into the MDOffloadBucket when we create it below.

  // Convert the attributes from the proto to rgw::sal::Attrs, then create an
  // MDOffloadBucket using the next->bucket and the attributes.
  auto Attrs = ::akamai::grpcutil::attrs_from_proto(response.attributes());
  Bucket* fb = new MDOffloadBucket(std::move(nb), u, this, std::move(Attrs));
  bucket->reset(fb);

  return 0;
}

std::unique_ptr<Writer> MDOffloadFilterDriver::get_append_writer(const DoutPrefixProvider* dpp,
    optional_yield y,
    rgw::sal::Object* obj,
    const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule,
    const std::string& unique_tag,
    uint64_t position,
    uint64_t* cur_accounted_size)
{
  std::unique_ptr<Writer> writer = next->get_append_writer(dpp, y, nextObject(obj), owner,
      ptail_placement_rule, unique_tag, position, cur_accounted_size);
  if (!writer) {
    return nullptr;
  }
  return std::make_unique<MDOffloadWriter>(std::move(writer), obj, this);
}

std::unique_ptr<Writer> MDOffloadFilterDriver::get_atomic_writer(const DoutPrefixProvider* dpp,
    optional_yield y,
    rgw::sal::Object* obj,
    const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule,
    uint64_t olh_epoch,
    const std::string& unique_tag)
{
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, nextObject(obj), owner,
      ptail_placement_rule, olh_epoch, unique_tag);
  if (!writer) {
    return nullptr;
  }
  return std::make_unique<MDOffloadWriter>(std::move(writer), obj, this);
}

/****************************************************************************/

// rgw::sal::MDoffloadUser

int MDOffloadUser::create_bucket(const DoutPrefixProvider* dpp,
    const rgw_bucket& b,
    const std::string& zonegroup_id,
    rgw_placement_rule& placement_rule,
    std::string& swift_ver_location,
    const RGWQuotaInfo* pquota_info,
    const RGWAccessControlPolicy& policy,
    Attrs& attrs,
    RGWBucketInfo& info,
    obj_version& ep_objv,
    bool exclusive,
    bool obj_lock_enabled,
    bool* existed,
    req_info& req_info,
    std::unique_ptr<Bucket>* bucket_out,
    optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;

  // Pass an empty set of attributes to the next driver.
  rgw::sal::Attrs empty_attrs;

  ret = next->create_bucket(dpp, b, zonegroup_id, placement_rule,
      swift_ver_location, pquota_info, policy, empty_attrs,
      info, ep_objv, exclusive, obj_lock_enabled, existed,
      req_info, &nb, y);
  if (ret < 0)
    return ret;

  if (!nb) {
    ldpp_dout(dpp, 0)
        << fmt::format(FMT_STRING("MDOffloadUser::create_bucket: next->create_bucket() returned null bucket for name={}"), b.name)
        << dendl;
    return -EINVAL;
  }

  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadUser::create_bucket: parent driver created bucket name={} id={}"),
             nb->get_name(), nb->get_bucket_id())
      << dendl;

  // Set the attributes for this bucket in the remote store.
  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();

  ::grpc::ClientContext context;
  mdoffload::v1::SetBucketAttributesRequest request;
  mdoffload::v1::SetBucketAttributesResponse response;

  request.set_user_id(get_display_name());

  // Get the bucket name and ID from the newly created bucket.
  request.set_bucket_name(nb->get_name());
  request.set_bucket_id(nb->get_bucket_id());

  for (const auto& it : attrs) {
    ldpp_dout(dpp, 20)
        << fmt::format(FMT_STRING("MDOffloadUser::create_bucket: setting attr '{}' ({} bytes)'"), it.first, it.second.length())
        << dendl;
    (*request.mutable_attributes_to_add())[it.first] = it.second.to_str();
  }
  auto status = client->stub()->SetBucketAttributes(&context, request, &response);
  if (!status.ok()) {
    ldpp_dout(dpp, 0) << fmt::format(FMT_STRING("MDOffloadUser::create_bucket: gRPC SetBucketAttributes failed: {}"),
        status.error_message())
                      << dendl;
    return -EINVAL; // XXX appropriate error code?
  }

  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadUser::create_bucket: name={} attrs={}"),
             b.name, attrs)
      << dendl;

  Bucket* fb = new MDOffloadBucket(std::move(nb), this, driver_);
  // Load the real attributes.
  fb->set_attrs(attrs);
  bucket_out->reset(fb);

  return 0;
}

/****************************************************************************/

// rgw::sal::MDoffloadBucket

Attrs& MDOffloadBucket::get_attrs()
{
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadBucket::get_attrs: cached_attrs_={}"),
             cached_attrs_)
      << dendl;
  return cached_attrs_;
}

/**
 * @brief Set the attributes for the bucket.
 *
 * This isn't called by upstream RGW at all, but we may call it as we see fit.
 * The reason it's not called by main RGW is that it always uses
 * merge_and_store_attrs() except for bucket creation, where the initial
 * attributes are set via create_bucket(). We have to override create_bucket()
 * and it's logical to call set_attrs() from there.
 *
 * @param a The attributes to set.
 * @return int 0 on success, negative error code on failure.
 */
int MDOffloadBucket::set_attrs(Attrs a)
{
  cached_attrs_ = a;
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadBucket::set_attrs: attrs={}"),
             cached_attrs_)
      << dendl;
  return 0;
}

int MDOffloadBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y)
{
  // Fetch attributes for this bucket.
  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  ::grpc::ClientContext context;
  mdoffload::v1::SetBucketAttributesRequest request;
  mdoffload::v1::SetBucketAttributesResponse response;

  request.set_bucket_name(get_name());
  request.set_bucket_id(get_bucket_id());
  for (const auto& it : new_attrs) {
    (*request.mutable_attributes_to_add())[it.first] = it.second.to_str();
  }

  auto status = client->stub()->SetBucketAttributes(&context, request, &response);
  if (!status.ok()) {
    ldpp_dout(dpp, 20) << fmt::format(FMT_STRING("MDOffloadBucket::merge_and_store_attrs: gRPC SetBucketAttributes failed: {}"),
        status.error_message())
                       << dendl;
    return -EINVAL; // XXX appropriate error code?
  }

  for (auto& it : new_attrs) {
    cached_attrs_[it.first] = it.second;
  }
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadBucket::merge_and_store_attrs: bucket='{}' id='{}' new_attrs='{}' cached_attrs_='{}'"),
             get_name(), get_bucket_id(), new_attrs, cached_attrs_)
      << dendl;
  return 0;
}

std::unique_ptr<Object> MDOffloadBucket::get_object(const rgw_obj_key& key)
{
  ldout(g_ceph_context, 20) << fmt::format(FMT_STRING("MDOffloadBucket::get_object: key={}"), key) << dendl;

  std::unique_ptr<Object> new_object = next->get_object(key);
  if (!new_object)
    return nullptr;

  // Wrap the Object in an MDOffloadObject.
  auto md_object = std::make_unique<MDOffloadObject>(std::move(new_object), this, driver_);
  return md_object;
}

/****************************************************************************/

// rgw::sal::MDOffloadObject

// No-bucket constructor. Set in req_state by init_from_header(). This
// persists in the req_state and is upgraded using [driver]::set_bucket()
// later, in init_permissions().
MDOffloadObject::MDOffloadObject(std::unique_ptr<Object> next, MDOffloadFilterDriver* driver)
    : FilterLogObject(std::move(next))
    , driver_(driver)
{
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadObject({}):: created object for (no bucket) key='{}'"), (void*)this, get_key())
      << dendl;
}

// Constructor with bucket.
MDOffloadObject::MDOffloadObject(std::unique_ptr<Object> next, Bucket* bucket, MDOffloadFilterDriver* driver)
    : FilterLogObject(std::move(next), bucket)
    , driver_(driver)
{
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadObject({}):: created object for bucket='{}' key='{}'"), (void*)this, bucket->get_name(), get_key())
      << dendl;
}

// 'Clone' constructor.
MDOffloadObject::MDOffloadObject(MDOffloadObject& _o)
    : FilterLogObject(_o)
{
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadObject({}):: clone from MDOffloadObject({}) for key='{}'"), (void*)this, (void*)&_o, get_key())
      << dendl;
  // Clone local fields.
  driver_ = _o.driver_;
};

// rgw::sal::MDOffloadObject::MDOffloadReadOp

std::unique_ptr<Object::ReadOp> MDOffloadObject::get_read_op()
{
  ldout(g_ceph_context, 20) << fmt::format(FMT_STRING("MDOffloadObject({})::get_read_op: key='{}'"), (void*)this, get_key()) << dendl;

  // Almost-duplicate of FilterLogObject::get_read_op() returning the correct
  // type.
  std::unique_ptr<ReadOp> r = next->get_read_op();

  auto ret = std::make_unique<MDOffloadReadOp>(std::move(r), this, get_bucket(), driver_);
  return ret;
}

int MDOffloadObject::MDOffloadReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  // Call the base class, then load all the attributes. This is the first
  // opportunity to work on object attributes.

  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::prepare pre-exec"))
      << dendl;
  // Rados ReadOp::prepare() will load xattrs.
  int ret = FilterLogObject::FilterLogReadOp::prepare(y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::prepare() failed ret={}"), ret) << dendl;
    return ret;
  }

  // If we're going to detect modified attributes we need to upload, now's the
  // time.
  auto& attr = object_->get_attrs();

  std::set<std::string> attr_must_export;

  // for (const auto& it : attr) {
  //   if (attr_is_exported(it.first)) {
  //     ldpp_dout(dpp, 1)
  //         << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::prepare: ERROR found existing exportable attr '{}' ({} bytes)'"), it.first, it.second.length())
  //         << dendl;
  //   }
  // }

  // Fetch the external attributes for this object from the remote store.
  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  ::grpc::ClientContext context;
  mdoffload::v1::GetObjectAttributesRequest request;
  mdoffload::v1::GetObjectAttributesResponse response;
  auto key = object_->get_key();
  request.set_bucket_name(bucket_->get_name());
  request.set_bucket_id(bucket_->get_bucket_id());
  request.set_object_key(key.name);
  request.set_object_instance_id(key.instance);
  auto status = client->stub()->GetObjectAttributes(&context, request, &response);
  if (!status.ok()) {
    ldpp_dout(dpp, 0) << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::prepare: gRPC GetObjectAttributes failed: {}"),
        status.error_message())
                      << dendl;
    return ERR_INTERNAL_ERROR; // XXX appropriate error code?
  }
  Attrs fetched_attrs = akamai::grpcutil::attrs_from_proto(response.attributes());
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::prepare: fetched attributes for bucket {} id {} object key='{}' attrs={}"),
             bucket_->get_name(), bucket_->get_bucket_id(), key, fetched_attrs)
      << dendl;

  // Loop through the attributes we fetched. If the attribute is marked for a
  // diff check, do so. Otherwise merge the attribute directly.
  for (const auto& it : fetched_attrs) {
    bool should_add = true;
    if (attr_needs_import_check(it.first)) {
      auto local_it = attr.find(it.first);
      if (local_it != attr.end()) {
        // Attribute exists locally, compare.
        if (local_it->second == it.second) {
          // No change.
          ldpp_dout(dpp, 20)
              << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::prepare: attribute '{}' unchanged"), it.first)
              << dendl;
          should_add = false;

        } else {
          // Changed.
          ldpp_dout(dpp, 1)
              << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::prepare: attribute '{}' CHANGED, updating"), it.first)
              << dendl;
        }
      }
    }
    if (should_add) {
      attr[it.first] = it.second;
    }
  }
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::prepare: merged attributes={}"), attr)
      << dendl;

  return 0;
}

int MDOffloadObject::MDOffloadReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
    optional_yield y, const DoutPrefixProvider* dpp)
{
  // Passthrough with logging.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::read: ofs={} end={}"), ofs, end)
      << dendl;
  return FilterLogObject::FilterLogReadOp::read(ofs, end, bl, y, dpp);
}

int MDOffloadObject::MDOffloadReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  // Passthrough with logging.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::get_attr: name='{}'"), name)
      << dendl;
  return FilterLogObject::FilterLogReadOp::get_attr(dpp, name, dest, y);
}

int MDOffloadObject::MDOffloadReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
    int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  // Passthrough with logging.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::iterate: ofs={} end={}"), ofs, end)
      << dendl;
  return FilterLogObject::FilterLogReadOp::iterate(dpp, ofs, end, cb, y);
}

// rgw::sal::MDOffloadObject::MDOffloadDeleteOp

std::unique_ptr<Object::DeleteOp> MDOffloadObject::get_delete_op()
{
  // Almost-duplicate of FilterLogObject::get_delete_op() returning the correct
  // type.
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<MDOffloadDeleteOp>(std::move(d), this, get_bucket(), driver_);
}

int MDOffloadObject::MDOffloadDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags)
{
  // XXX placeholder: delete upstream attributes, or at least mark as
  // deleting.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadDeleteOp::delete_obj: flags={}"), flags)
      << dendl;
  int r = FilterLogObject::FilterLogDeleteOp::delete_obj(dpp, y, flags);
  return r;
}

int MDOffloadObject::delete_object(const DoutPrefixProvider* dpp,
    optional_yield y,
    uint32_t flags)
{
  // Passthrough with logging.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::delete_object: key='{}' flags={}"), get_key(), flags)
      << dendl;
  return next->delete_object(dpp, y, flags);
}

int MDOffloadObject::delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
    bool keep_index_consistent, optional_yield y)
{
  // Passthrough with logging.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::delete_obj_aio: key='{}' keep_index_consistent={}"), get_key(), keep_index_consistent)
      << dendl;
  return next->delete_obj_aio(dpp, astate, aio, keep_index_consistent, y);
}

int MDOffloadObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y)
{
  // The Rados driver uses this to set attributes in the backing store.
  //
  // Note the Rados driver call modifies the attr mtime, here's the comment:
  //
  //// make a tiny adjustment to the existing mtime so that fetch_remote_obj()
  //// won't return ERR_NOT_MODIFIED when syncing the modified object
  //
  // Rados increments the timestamp by 1 microsecond, and we should do the
  // same if we're storing a specific mtime. Ideally we'd automatically store
  // an mtime on the attributes and it wouldn't have to be a separate item.

  // Send our gRPC to set the attributes.
  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  ::grpc::ClientContext context;
  mdoffload::v1::SetObjectAttributesRequest request;
  mdoffload::v1::SetObjectAttributesResponse response;
  Bucket* bucket = get_bucket();
  Object* next_obj = get_next();

  request.set_bucket_name(bucket->get_name());
  request.set_bucket_id(bucket->get_bucket_id());
  request.set_object_key(next_obj->get_key().name);
  request.set_object_instance_id(next_obj->get_key().instance);
  if (setattrs != nullptr) {
    for (const auto& it : *setattrs) {
      ldpp_dout(dpp, 20)
          << fmt::format(FMT_STRING("MDOffloadObject::set_obj_attrs: setting attr '{}' ({} bytes)'"), it.first, it.second.length())
          << dendl;
      (*request.mutable_attributes_to_add())[it.first] = it.second.to_str();
    }
  }
  if (delattrs != nullptr) {
    for (const auto& it : *delattrs) {
      ldpp_dout(dpp, 20)
          << fmt::format(FMT_STRING("MDOffloadObject::set_obj_attrs: deleting attr '{}'"), it.first)
          << dendl;
      request.add_attributes_to_delete(it.first);
      ;
    }
  }
  auto status = client->stub()->SetObjectAttributes(&context, request, &response);
  if (!status.ok()) {
    ldpp_dout(dpp, 20) << fmt::format(FMT_STRING("MDOffloadObject::set_obj_attrs: gRPC SetObjectAttributes failed: {}"),
        status.error_message())
                       << dendl;
    return -EINVAL; // XXX appropriate error code?
  }

  // // Only after gRPC success do we modify our cached attributes.
  // Attrs new_attrs = cached_attrs_;
  // if (setattrs != nullptr) {
  //   for (const auto& it : *setattrs) {
  //     new_attrs[it.first] = it.second;
  //   }
  // }
  // if (delattrs != nullptr) {
  //   for (const auto& it : *delattrs) {
  //     new_attrs.erase(it.first);
  //   }
  // }
  // ldpp_dout(dpp, 20)
  //     << fmt::format(FMT_STRING("MDOffloadObject::set_obj_attrs: setattrs={} delattrs={} cached_attrs_={}"),
  //            fmt_maybe(setattrs),
  //            fmt_maybe(delattrs),
  //            cached_attrs_)
  //     << dendl;
  // cached_attrs_ = new_attrs;
  // has_attrs_ = true;

  int ret = FilterLogObject::set_obj_attrs(dpp, setattrs, delattrs, y);
  if (ret < 0) {
    // XXX uh-oh - what do we do here? We've already modified the remote.
    // XXX FIXME
    ldpp_dout(dpp, 20)
        << fmt::format(FMT_STRING("MDOffloadObject::set_obj_attrs: FilterLogObject::set_obj_attrs() failed: {}"), ret)
        << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::set_obj_attrs: set_attrs() attrs={}"),
             FilterLogObject::get_attrs())
      << dendl;
  return 0;
}

int MDOffloadObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj)
{
  // The Rados driver fetches the attributes from the backing store using this
  // call.

  // Send our gRPC to get the attributes.
  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  ::grpc::ClientContext context;
  mdoffload::v1::GetObjectAttributesRequest request;
  mdoffload::v1::GetObjectAttributesResponse response;
  Bucket* bucket = get_bucket();
  Object* next_obj = get_next();
  request.set_bucket_name(bucket->get_name());
  request.set_bucket_id(bucket->get_bucket_id());
  request.set_object_key(next_obj->get_key().name);
  request.set_object_instance_id(next_obj->get_key().instance);
  auto status = client->stub()->GetObjectAttributes(&context, request, &response);
  if (!status.ok()) {
    ldpp_dout(dpp, 20) << fmt::format(FMT_STRING("MDOffloadObject::get_obj_attrs: gRPC GetObjectAttributes failed: {}"),
        status.error_message())
                       << dendl;
    return -EINVAL; // XXX appropriate error code?
  }

  auto new_attrs = rgw::sal::Attrs {};
  for (const auto& kv : response.attributes()) {
    bufferlist bl;
    bl.append(kv.second);
    new_attrs[kv.first] = std::move(bl);
  }

  // cached_attrs_ = std::move(new_attrs);
  // has_attrs_ = true;
  // ldpp_dout(dpp, 20)
  //     << fmt::format(FMT_STRING("MDOffloadObject::get_obj_attrs: cached_attrs_={}"),
  //            cached_attrs_)
  //     << dendl;

  // XXX passthrough
  FilterLogObject::set_attrs(std::move(new_attrs));
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::get_obj_attrs: set_attrs() passthrough attrs={}"),
             FilterLogObject::get_attrs())
      << dendl;

  return 0;
}
int MDOffloadObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp)
{
  // The Rados driver uses this to modify a single attribute. Note that an
  // attribute's use may be more than just a simple key/value pair; for
  // example, tags are all stored on the single attribute
  // user.rgw.z-amz-tagging.

  // NOTE the Rados driver call to set_atomic() when modifying attributes. We
  // need to be VERY CAREFUL to not modify the upstream object's invariants;
  // changes are we may have to make that call here too, without the attr
  // changes.

  // Send our gRPC to set the attribute.
  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  ::grpc::ClientContext context;
  mdoffload::v1::SetObjectAttributesRequest request;
  mdoffload::v1::SetObjectAttributesResponse response;
  Bucket* bucket = get_bucket();
  Object* next_obj = get_next();

  request.set_bucket_name(bucket->get_name());
  request.set_bucket_id(bucket->get_bucket_id());
  request.set_object_key(next_obj->get_key().name);
  request.set_object_instance_id(next_obj->get_key().instance);
  (*request.mutable_attributes_to_add())[attr_name] = attr_val.to_str();

  auto status = client->stub()->SetObjectAttributes(&context, request, &response);
  if (!status.ok()) {
    ldpp_dout(dpp, 20) << fmt::format(FMT_STRING("MDOffloadObject::modify_obj_attrs: gRPC SetObjectAttributes failed: {}"),
        status.error_message())
                       << dendl;
    return -EINVAL; // XXX appropriate error code?
  }

  // // Only after gRPC success do we modify our cached attributes.
  // Attrs new_attrs = cached_attrs_;
  // ldpp_dout(dpp, 20)
  //     << fmt::format(FMT_STRING("MDOffloadObject::modify_obj_attrs: attr_name={} attr_val[{} bytes]"),
  //            attr_name, attr_val.length())
  //     << dendl;
  // new_attrs[attr_name] = attr_val;
  // cached_attrs_ = new_attrs;

  // XXX passthrough
  auto& attrs = FilterLogObject::get_attrs();
  attrs[attr_name] = attr_val;
  // set_attrs(attrs);
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::modify_obj_attrs: set_attrs() attrs={}"),
             attrs)
      << dendl;

  return 0;
}

int MDOffloadObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y)
{
  // The Rados driver uses this to delete a single attribute. It does it via
  // set_obj_attrs(), and we should do the same.

  // NOTE the Rados driver call to set_atomic() when modifying attributes. We
  // need to be VERY CAREFUL to not modify the upstream object's invariants;
  // changes are we may have to make that call here too, without the attr
  // changes.

  if (!MDOffloadObject::attr_is_exported(attr_name)) {
    // Passthrough with logging.
    COND_LOG_D(dpp, 20, "MDOffloadObject::delete_obj_attrs: non-exported attr_name='{}', passthrough", attr_name);
    return FilterLogObject::delete_obj_attrs(dpp, attr_name, y);

  } else {
    // Send our gRPC to delete the attribute.
    auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
    ::grpc::ClientContext context;
    mdoffload::v1::SetObjectAttributesRequest request;
    mdoffload::v1::SetObjectAttributesResponse response;
    Bucket* bucket = get_bucket();
    Object* next_obj = get_next();

    request.set_bucket_name(bucket->get_name());
    request.set_bucket_id(bucket->get_bucket_id());
    request.set_object_key(next_obj->get_key().name);
    request.set_object_instance_id(next_obj->get_key().instance);
    request.add_attributes_to_delete(attr_name);

    auto status = client->stub()->SetObjectAttributes(&context, request, &response);
    if (!status.ok()) {
      COND_LOG_D(dpp, 20, "MDOffloadObject::delete_obj_attrs: gRPC SetObjectAttributes failed: {}", status.error_message());
      return -EINVAL; // XXX appropriate error code?
    }

    // Only after gRPC success do we modify our cached attributes.
    Attrs rmattr;
    rmattr[attr_name] = bufferlist();

    COND_LOG_D(dpp, 20, "MDOffloadObject::delete_obj_attrs: attr_name={}", attr_name);
    return FilterLogObject::set_obj_attrs(dpp, nullptr, &rmattr, y);
  }
}

Attrs& MDOffloadObject::get_attrs(void)
{
  // // XXX placeholder
  // return cached_attrs_;

  // XXX passthrough
  return FilterLogObject::get_attrs();

  // // XXX !!! Need some caching here, at the moment try to use the underlying
  // // object to pull the RADOS-only attributes.

  // // Pull the attributes from the underlying object
  // auto& real_attr = FilterLogObject::get_attrs();
  // if (get_bucket()) {

  //   ldout(g_ceph_context, 20)
  //       << fmt::format(FMT_STRING("MDOffloadObject::get_attrs: bucket={} bucket_id={} attrs={}"),
  //              get_bucket()->get_name(), get_bucket()->get_bucket_id(), real_attr)
  //       << dendl;

  //   auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  //   ::grpc::ClientContext context;
  //   mdoffload::v1::GetObjectAttributesRequest request;
  //   mdoffload::v1::GetObjectAttributesResponse response;
  //   auto key = get_key();
  //   request.set_bucket_name(get_bucket()->get_name());
  //   request.set_bucket_id(get_bucket()->get_bucket_id());
  //   request.set_object_key(key.name);
  //   request.set_object_instance_id(key.instance);
  //   auto status = client->stub()->GetObjectAttributes(&context, request, &response);
  //   if (status.ok()) {
  //     ldout(g_ceph_context, 20)
  //         << fmt::format(FMT_STRING("MDOffloadObject::get_attrs: fetched attributes for object key='{}' attrs={}"),
  //                key, ::akamai::grpcutil::attrs_from_proto(response.attributes()))
  //         << dendl;
  //     for (const auto& kv : response.attributes()) {

  //       auto item = real_attr.find(kv.first);
  //       if (item != real_attr.end()) {
  //         ldout(g_ceph_context, 20)
  //             << fmt::format(FMT_STRING("MDOffloadObject::get_attrs: XXX overridden rados attr found attr='{}' rados={} ext={}"),
  //                    kv.first, item->second.length(), kv.second.length())
  //             << dendl;
  //       }
  //       bufferlist bl;
  //       bl.append(kv.second);
  //       real_attr[kv.first] = std::move(bl);
  //     }
  //     ldout(g_ceph_context, 20)
  //         << fmt::format(FMT_STRING("MDOffloadObject::get_attrs: merged attrs={}"),
  //                real_attr)
  //         << dendl;
  //     return real_attr;

  //   } else {

  //     ldout(g_ceph_context, 20) << fmt::format(FMT_STRING("MDOffloadObject::get_attrs: gRPC GetObjectAttributes failed: {}"),
  //         status.error_message())
  //                               << dendl;
  //     return real_attr; // XXX no merge - WE NEED TO FAIL HERE
  //   }

  // } else {
  //   ldout(g_ceph_context, 20)
  //       << fmt::format(FMT_STRING("MDOffloadObject::get_attrs: bucket=<null> attrs={}"), real_attr)
  //       << dendl;
  // }
  // return real_attr;
}

const Attrs& MDOffloadObject::get_attrs(void) const
{
  // // XXX placeholder
  // return cached_attrs_;

  // XXX passthrough
  const auto& a = FilterLogObject::get_attrs();
  if (get_bucket()) {
    ldout(g_ceph_context, 20)
        << fmt::format(FMT_STRING("MDOffloadObject::get_attrs (const): bucket={} bucket_id={} attrs={}"),
               get_bucket()->get_name(), get_bucket()->get_bucket_id(), a)
        << dendl;
  } else {
    ldout(g_ceph_context, 20)
        << fmt::format(FMT_STRING("MDOffloadObject::get_attrs (const): bucket=<null> attrs={}"), a)
        << dendl;
  }
  return a;
}

int MDOffloadObject::set_attrs(Attrs a)
{
  // Passthrough.
  return FilterLogObject::set_attrs(std::move(a));
}

bool MDOffloadObject::has_attrs(void)
{
  // Passthrough.
  return FilterLogObject::has_attrs();
}

void MDOffloadObject::set_bucket(Bucket* b)
{
  COND_LOG_G(20, "MDOffloadObject({})::set_bucket: bucket='{}' key='{}'", (void*)this, b ? b->get_name() : std::string("<null>"), get_key());
  rgw::sal::FilterLogObject::set_bucket(b);
}

/****************************************************************************/

// MDOffloadWriter

int MDOffloadWriter::prepare(optional_yield y)
{
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadWriter::prepare: object='{}'"),
             obj ? obj->get_name() : std::string("<null>"))
      << dendl;
  return FilterLogWriter::prepare(y);
}

int MDOffloadWriter::process(bufferlist&& data, uint64_t offset)
{
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadWriter::process: object='{}' offset={} len={}"),
             obj ? obj->get_name() : std::string("<null>"), offset, data.length())
      << dendl;
  return FilterLogWriter::process(std::move(data), offset);
}

int MDOffloadWriter::complete(size_t accounted_size, const std::string& etag,
    ceph::real_time* mtime, ceph::real_time set_mtime,
    std::map<std::string, bufferlist>& attrs, ceph::real_time delete_at,
    const char* if_match, const char* if_nomatch, const std::string* user_data,
    rgw_zone_set* zones_trace, bool* canceled, optional_yield y, uint32_t flags)
{
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadWriter::complete: object='{}' accounted_size={} flags={} attrs={}"),
             obj ? obj->get_name() : std::string("<null>"), accounted_size, flags, attrs)
      << dendl;

  // Send the full set of attributes to the remote store.
  auto object = obj;
  if (!object) {
    // XXX Can this really happen?
    ldout(g_ceph_context, 0)
        << fmt::format(FMT_STRING("MDOffloadWriter::complete: no object"))
        << dendl;
    return -EINVAL; // XXX appropriate error code?
  }
  auto bucket = obj->get_bucket();
  if (!bucket) {
    ldout(g_ceph_context, 0)
        << fmt::format(FMT_STRING("MDOffloadWriter::complete: no bucket for object='{}'"), obj ? obj->get_name() : std::string("<null>"))
        << dendl;
    return -EINVAL; // XXX appropriate error code?
  }

  auto msg_prefix = fmt::format(FMT_STRING("MDOffloadWriter::complete: bucket='{}' id={} object='{}'"),
      obj->get_bucket()->get_name(),
      obj->get_bucket()->get_bucket_id(),
      obj ? obj->get_name() : std::string("<null>"));

  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  ::grpc::ClientContext context;
  mdoffload::v1::SetObjectAttributesRequest request;
  mdoffload::v1::SetObjectAttributesResponse response;
  // Defer setting bucket and object info until after we know we have attrs to
  // export.

  int attr_count = 0;

  for (const auto& it : attrs) {
    if (MDOffloadObject::attr_is_exported(it.first)) {
      ldout(g_ceph_context, 20)
          << fmt::format(FMT_STRING("{}: setting attr '{}' ({} bytes)'"), msg_prefix, it.first, it.second.length())
          << dendl;
      (*request.mutable_attributes_to_add())[it.first] = it.second.to_str();
      attr_count++;
    } else {
      ldout(g_ceph_context, 20)
          << fmt::format(FMT_STRING("MDOffloadWriter::complete: skipping non-exported attr '{}'"), it.first)
          << dendl;
    }
  }
  if (attr_count == 0) {
    ldout(g_ceph_context, 20)
        << fmt::format(FMT_STRING("{}: no exported attributes to set for object='{}'"), msg_prefix, object->get_name())
        << dendl;
  } else {

    request.set_bucket_name(bucket->get_name());
    request.set_bucket_id(bucket->get_bucket_id());
    request.set_object_key(object->get_key().name);
    request.set_object_instance_id(object->get_key().instance);
    request.set_new_object_instance(true); // Indicate this is a new object version.

    auto status = client->stub()->SetObjectAttributes(&context, request, &response);
    if (!status.ok()) {
      ldout(g_ceph_context, 0) << fmt::format(FMT_STRING("{}: gRPC SetObjectAttributes failed: {}"),
          msg_prefix, status.error_message())
                               << dendl;
      return -ERR_INTERNAL_ERROR; // XXX appropriate error code?
    }
  }

  // ldout(g_ceph_context, 20)
  //     << fmt::format(FMT_STRING("MDOffloadWriter::complete: object='{}' WRITE EMPTY ATTRS "),
  //            object->get_name(), attrs)
  //     << dendl;
  // Attrs empty_attrs;
  // return FilterLogWriter::complete(accounted_size, etag, mtime, set_mtime, empty_attrs,
  //     delete_at, if_match, if_nomatch, user_data, zones_trace, canceled, y,
  //     flags);

  // ldout(g_ceph_context, 20)
  //     << fmt::format(FMT_STRING("MDOffloadWriter::complete: object='{}' WRITE COMPLETE ATTRS "),
  //            object->get_name(), attrs)
  //     << dendl;
  // return FilterLogWriter::complete(accounted_size, etag, mtime, set_mtime, attrs,
  //     delete_at, if_match, if_nomatch, user_data, zones_trace, canceled, y,
  //     flags);

  for (const auto& it : attrs) {
    if (MDOffloadObject::attr_import_prohibited(it.first)) {
      ldout(g_ceph_context, 1)
          << fmt::format(FMT_STRING("{}: removing external-only attribute '{}' from local attrs"),
                 msg_prefix, it.first)
          << dendl;
      attrs.erase(it.first);
    }
  }
  return FilterLogWriter::complete(accounted_size, etag, mtime, set_mtime, attrs,
      delete_at, if_match, if_nomatch, user_data, zones_trace, canceled, y,
      flags);
}

// A list of the attributes that are exported to the remote store.
static std::set<std::string> attr_object_exported = {
  RGW_ATTR_ACL,
  RGW_ATTR_CRYPT_CONTEXT,
  RGW_ATTR_CRYPT_DATAKEY,
  RGW_ATTR_CRYPT_KEYID,
  RGW_ATTR_CRYPT_KEYMD5,
  RGW_ATTR_CRYPT_KEYSEL,
  RGW_ATTR_CRYPT_MODE,
  RGW_ATTR_CRYPT_PARTS,
  RGW_ATTR_CRYPT_PREFIX,
  RGW_ATTR_ETAG,
  RGW_ATTR_STORAGE_CLASS,
  RGW_ATTR_TAGS,
};

bool MDOffloadObject::attr_is_exported(const std::string& attr_name)
{
  return attr_object_exported.contains(attr_name);
}

// Attributes we want to check for changes on import. We really, really don't
// want attributes to change, or we're asking for sync trouble.
static std::set<std::string> attr_object_import_check = {
  RGW_ATTR_CRYPT_DATAKEY,
  RGW_ATTR_CRYPT_PREFIX,
  RGW_ATTR_CRYPT_PARTS,
  RGW_ATTR_CRYPT_KEYID,
  RGW_ATTR_CRYPT_KEYSEL,
  RGW_ATTR_CRYPT_CONTEXT,
  RGW_ATTR_CRYPT_KEYMD5,
  RGW_ATTR_CRYPT_MODE,
  RGW_ATTR_ETAG,
};

bool MDOffloadObject::attr_needs_import_check(const std::string& attr_name)
{

  return attr_object_import_check.contains(attr_name);
}

// Attributes we don't want stored in Rados. This means we intercept them in
// Writer::prepare() and remove them from the Attrs passed to the next driver.
static std::set<std::string> attr_object_import_prohibited = {
  RGW_ATTR_CRYPT_DATAKEY,
  RGW_ATTR_CRYPT_PREFIX,
  RGW_ATTR_CRYPT_PARTS,
  RGW_ATTR_CRYPT_KEYID,
  RGW_ATTR_CRYPT_KEYSEL,
  RGW_ATTR_CRYPT_CONTEXT,
  RGW_ATTR_CRYPT_KEYMD5,
  RGW_ATTR_CRYPT_MODE,
  RGW_ATTR_TAGS,
};

bool MDOffloadObject::attr_import_prohibited(const std::string& attr_name)
{
  return attr_object_import_prohibited.contains(attr_name);
}

/****************************************************************************/

} // namespace rgw::sal

rgw::sal::Driver* newMDOffloadFilter(CephContext* cct, rgw::sal::Driver* next)
{
  if (next == nullptr) {
    throw rgw::sal::MDOffloadFilterDriver::BadNextDriver(fmt::format(FMT_STRING("{}: next driver cannot be nullptr"), __func__));
  }

  rgw::sal::MDOffloadFilterDriver* driver = new rgw::sal::MDOffloadFilterDriver(cct, next);

  return driver;
}
