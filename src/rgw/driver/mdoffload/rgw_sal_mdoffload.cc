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
#include "mdoffload/v1/mdoffload.pb.h"
#include "rgw_common.h"
#include "rgw_sal.h"

#include "mdoffload_logutil.h"

#define dout_subsys ceph_subsys_rgw

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
    LOG_PFX(dpp, 0, "MDOffloadFilterDriver::initialize: no gRPC URI configured");
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
  COND_LOG_G(20, "MDOffloadFilterDriver({})::get_object: rgw_obj_key k={}", (void*)this, k);
  std::unique_ptr<Object> o = next->get_object(k);
  if (!o) {
    // This really shouldn't happen, but log it if it does.
    LOG_G(0, "MDOffloadFilterDriver::get_object: next->get_object() failed for key {}", k);
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

  COND_LOG_PFX(dpp, 20, "MDOffloadFilterDriver::get_bucket: (variant 1) rgw_bucket b={} nu={}", b,
      fmt_maybe(nu));

  ret = next->get_bucket(dpp, nu, b, &nb, y);
  if (ret != 0)
    return ret;

  // Bucket exists. Need to preload the bucket attributes.
  COND_LOG_PFX(dpp, 20, "MDOffloadFilterDriver::get_bucket: (variant 3) fetched bucket name={} id={}",
      nb->get_name(), nb->get_bucket_id());

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
    LOG_PFX(dpp, 0, "MDOffloadFilterDriver::get_bucket: (variant 3) gRPC GetBucketAttributes failed: {}", status.error_message());
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

  COND_LOG_G(20, "MDOffloadFilterDriver::get_bucket: (variant 2) RGWBucketInfo i={} nu={}", i,
      fmt_maybe(nu));

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

  COND_LOG_PFX(dpp, 20, "MDOffloadFilterDriver::get_bucket: (variant 3) tenant={} name={} nu={}", tenant, name,
      fmt_maybe(nu));

  ret = next->get_bucket(dpp, nu, tenant, name, &nb, y);
  if (ret != 0)
    return ret;

  // Bucket exists. Need to preload the bucket attributes.
  COND_LOG_PFX(dpp, 20, "MDOffloadFilterDriver::get_bucket: (variant 3) fetched bucket name={} id={}",
      nb->get_name(), nb->get_bucket_id());

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
    LOG_PFX(dpp, 0, "MDOffloadFilterDriver::get_bucket: (variant 3) gRPC GetBucketAttributes failed: {}", status.error_message());
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
    LOG_PFX(dpp, 0, "MDOffloadUser::create_bucket: next->create_bucket() returned null bucket for name={}", b.name);
    return -EINVAL;
  }

  COND_LOG_PFX(dpp, 20, "MDOffloadUser::create_bucket: parent driver created bucket name={} id={}",
      nb->get_name(), nb->get_bucket_id());

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
    COND_LOG_PFX(dpp, 20, "MDOffloadUser::create_bucket: setting attr '{}' ({} bytes)'", it.first, it.second.length());
    (*request.mutable_attributes_to_add())[it.first] = it.second.to_str();
  }
  auto status = client->stub()->SetBucketAttributes(&context, request, &response);
  if (!status.ok()) {
    LOG_PFX(dpp, 0, "MDOffloadUser::create_bucket: gRPC SetBucketAttributes failed: {}", status.error_message());
    return -EINVAL; // XXX appropriate error code?
  }

  COND_LOG_PFX(dpp, 20, "MDOffloadUser::create_bucket: name={} id={} attrs={}",
      nb->get_name(), nb->get_bucket_id(), attrs);

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
  COND_LOG_G(20, "MDOffloadBucket::get_attrs: cached_attrs_={}", cached_attrs_);
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
  COND_LOG_G(20, "MDOffloadBucket::set_attrs: attrs={}", cached_attrs_);
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
    COND_LOG_G(20, "MDOffloadBucket::merge_and_store_attrs: gRPC SetBucketAttributes failed: {}", status.error_message());
    return -EINVAL; // XXX appropriate error code?
  }

  for (auto& it : new_attrs) {
    cached_attrs_[it.first] = it.second;
  }
  COND_LOG_G(20, "MDOffloadBucket::merge_and_store_attrs: bucket='{}' id='{}' new_attrs='{}' cached_attrs_='{}'",
      get_name(), get_bucket_id(), new_attrs, cached_attrs_);
  return 0;
}

std::unique_ptr<Object> MDOffloadBucket::get_object(const rgw_obj_key& key)
{
  COND_LOG_G(20, "MDOffloadBucket::get_object: key={}", key);

  std::unique_ptr<Object> new_object = next->get_object(key);
  if (!new_object)
    return nullptr;

  // Wrap the Object in an MDOffloadObject.
  auto md_object = std::make_unique<MDOffloadObject>(std::move(new_object), this, driver_);
  return md_object;
}

std::unique_ptr<MultipartUpload> MDOffloadBucket::get_multipart_upload(
    const std::string& oid,
    std::optional<std::string> upload_id,
    ACLOwner owner, ceph::real_time mtime)
{
  std::unique_ptr<MultipartUpload> nmu = next->get_multipart_upload(oid, upload_id, owner, mtime);

  return std::make_unique<MDOffloadMultipartUpload>(std::move(nmu), this, driver_);
}

int MDOffloadBucket::list_multiparts(const DoutPrefixProvider* dpp,
    const std::string& prefix,
    std::string& marker,
    const std::string& delim,
    const int& max_uploads,
    std::vector<std::unique_ptr<MultipartUpload>>& uploads,
    std::map<std::string, bool>* common_prefixes,
    bool* is_truncated)
{
  std::vector<std::unique_ptr<MultipartUpload>> nup;
  int ret;

  ret = next->list_multiparts(dpp, prefix, marker, delim, max_uploads, nup,
      common_prefixes, is_truncated);
  if (ret < 0)
    return ret;

  for (auto& ent : nup) {
    uploads.emplace_back(std::make_unique<MDOffloadMultipartUpload>(std::move(ent), this, driver_));
  }

  return 0;
}

int MDOffloadBucket::abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct)
{
  return next->abort_multiparts(dpp, cct);
}

/****************************************************************************/

// rgw::sal::MDOffloadObject

// No-bucket constructor. Set in req_state by init_from_header(). This
// persists in the req_state and is upgraded using [driver]::set_bucket()
// later, in init_permissions().
MDOffloadObject::MDOffloadObject(std::unique_ptr<Object> next, MDOffloadFilterDriver* driver)
    : MDOFilterParentObject(std::move(next))
    , driver_(driver)
{
  COND_LOG_G(20, "MDOffloadObject({}):: created object for (no bucket) key={}", (void*)this, get_key());
}

// Constructor with bucket.
MDOffloadObject::MDOffloadObject(std::unique_ptr<Object> next, Bucket* bucket, MDOffloadFilterDriver* driver)
    : MDOFilterParentObject(std::move(next), bucket)
    , driver_(driver)
{
  COND_LOG_G(20, "MDOffloadObject({}):: created object for bucket='{}' key={}",
      (void*)this, bucket->get_name(), get_key());
}

// 'Clone' constructor.
MDOffloadObject::MDOffloadObject(MDOffloadObject& _o)
    : MDOFilterParentObject(_o)
{
  COND_LOG_G(20, "MDOffloadObject({}):: clone from MDOffloadObject({}) for key={}", (void*)this, (void*)&_o, get_key());
  // Clone local fields.
  driver_ = _o.driver_;
};

// rgw::sal::MDOffloadObject::MDOffloadReadOp

std::unique_ptr<Object::ReadOp> MDOffloadObject::get_read_op()
{
  COND_LOG_G(20, "MDOffloadObject::get_read_op: key={}", get_key());

  // Almost-duplicate of MDOFilter*Object::get_read_op() returning the correct
  // type.
  std::unique_ptr<ReadOp> r = next->get_read_op();

  auto ret = std::make_unique<MDOffloadReadOp>(std::move(r), this, get_bucket(), driver_);
  return ret;
}

int MDOffloadObject::MDOffloadReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  // Call the base class, then load all the attributes. This is the first
  // opportunity to work on object attributes.

  COND_LOG_PFX(dpp, 20, "MDOffloadObject::MDOffloadReadOp::prepare: pre-exec");

  // Rados ReadOp::prepare() will load xattrs.
  int ret = MDOFilterParentObjectReadOp::prepare(y, dpp);
  if (ret < 0) {
    LOG_PFX(dpp, 0, "MDOffloadObject::MDOffloadReadOp::prepare: MDOFilter*ReadOp::prepare() failed ret={}", ret);
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
    LOG_PFX(dpp, 0, "MDOffloadObject::MDOffloadReadOp::prepare: gRPC GetObjectAttributes failed: {}", status.error_message());
    return ERR_INTERNAL_ERROR; // XXX appropriate error code?
  }
  Attrs fetched_attrs = akamai::grpcutil::attrs_from_proto(response.attributes());
  COND_LOG_PFX(dpp, 20, "MDOffloadObject::MDOffloadReadOp::prepare: fetched attributes for bucket {} id {} object key={} attrs={}",
      bucket_->get_name(), bucket_->get_bucket_id(), key, fetched_attrs);

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
          COND_LOG_PFX(dpp, 20, "MDOffloadObject::MDOffloadReadOp::prepare: attribute '{}' unchanged", it.first);
          should_add = false;

        } else {
          // Changed.
          LOG_PFX(dpp, 1, "MDOffloadObject::MDOffloadReadOp::prepare: attribute '{}' CHANGED, updating", it.first);
        }
      }
    }
    if (should_add) {
      attr[it.first] = it.second;
    }
  }
  COND_LOG_PFX(dpp, 20, "MDOffloadObject::MDOffloadReadOp::prepare: merged attributes={}", attr);

  return 0;
}

int MDOffloadObject::MDOffloadReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
    optional_yield y, const DoutPrefixProvider* dpp)
{
  // Passthrough with logging.
  COND_LOG_PFX(dpp, 20, "MDOffloadObject::MDOffloadReadOp::read: ofs={} end={}", ofs, end);
  return MDOFilterParentObjectReadOp::read(ofs, end, bl, y, dpp);
}

int MDOffloadObject::MDOffloadReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  // Passthrough with logging.
  COND_LOG_PFX(dpp, 20, "MDOffloadObject::MDOffloadReadOp::get_attr: name='{}'", name);
  return MDOFilterParentObjectReadOp::get_attr(dpp, name, dest, y);
}

int MDOffloadObject::MDOffloadReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
    int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  // Passthrough with logging.
  COND_LOG_PFX(dpp, 20, "MDOffloadObject::MDOffloadReadOp::iterate: ofs={} end={}", ofs, end);
  return MDOFilterParentObjectReadOp::iterate(dpp, ofs, end, cb, y);
}

// rgw::sal::MDOffloadObject::MDOffloadDeleteOp

std::unique_ptr<Object::DeleteOp> MDOffloadObject::get_delete_op()
{
  // Almost-duplicate of Filter*Object::get_delete_op() returning the correct
  // type.
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<MDOffloadDeleteOp>(std::move(d), this, get_bucket(), driver_);
}

int MDOffloadObject::MDOffloadDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags)
{
  COND_LOG_PFX(dpp, 20, "MDOffloadObject::MDOffloadDeleteOp::delete_obj: bucket={} key={} flags={}",
      bucket_->get_name(), object_->get_key(), flags);

  auto ret = MDOFilterParentObjectDeleteOp::delete_obj(dpp, y, flags);
  if (ret < 0) {
    LOG_PFX(dpp, 0, "MDOffloadObject::MDOffloadDeleteOp::delete_obj: Filter*DeleteOp::delete_obj() failed ret={}", ret);
    return ret;
  }

  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  ::grpc::ClientContext context;
  mdoffload::v1::PurgeObjectAttributesRequest request;
  mdoffload::v1::PurgeObjectAttributesResponse response;
  auto key = object_->get_key();
  request.set_bucket_name(bucket_->get_name());
  request.set_bucket_id(bucket_->get_bucket_id());
  request.set_object_key(key.name);
  request.set_object_instance_id(key.instance);
  auto status = client->stub()->PurgeObjectAttributes(&context, request, &response);
  if (!status.ok()) {
    LOG_PFX(dpp, 0, "MDOffloadObject::MDOffloadDeleteOp::delete_obj: gRPC PurgeObjectAttributes failed (object still deleted): {}", status.error_message());
  }
  return 0;
}

int MDOffloadObject::delete_object(const DoutPrefixProvider* dpp,
    optional_yield y,
    uint32_t flags)
{
  // RGWCompleteMultipart::execute() calls this instead of using the DeleteOp,
  // so it needs the gRPC callout.

  COND_LOG_PFX(dpp, 20, "MDOffloadObject::delete_object: bucket={} key={} flags={}",
      get_bucket()->get_name(), get_key(), flags);

  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  ::grpc::ClientContext context;
  mdoffload::v1::PurgeObjectAttributesRequest request;
  mdoffload::v1::PurgeObjectAttributesResponse response;
  auto key = get_key();
  request.set_bucket_name(get_bucket()->get_name());
  request.set_bucket_id(get_bucket()->get_bucket_id());
  request.set_object_key(key.name);
  request.set_object_instance_id(key.instance);
  auto status = client->stub()->PurgeObjectAttributes(&context, request, &response);
  if (!status.ok()) {
    LOG_PFX(dpp, 0, "MDOffloadObject::delete_object: gRPC PurgeObjectAttributes failed: {}", status.error_message());
    // Fall through; we still want to delete the object even
    // if we can't purge the attributes.
  }

  return next->delete_object(dpp, y, flags);
}

int MDOffloadObject::delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
    bool keep_index_consistent, optional_yield y)
{
  COND_LOG_PFX(dpp, 20, "MDOffloadObject::delete_obj_aio: bucket={} key={} keep_index_consistent={}",
      get_bucket()->get_name(), get_key(), keep_index_consistent);

  // XXX NO CODE CALLS THIS! (In v18.2.7) Should we implement attribute
  // purging here too?

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
      COND_LOG_PFX(dpp, 20, "MDOffloadObject::set_obj_attrs: deleting attr '{}'", it.first);
      request.add_attributes_to_delete(it.first);
      ;
    }
  }
  auto status = client->stub()->SetObjectAttributes(&context, request, &response);
  if (!status.ok()) {
    COND_LOG_PFX(dpp, 20, "MDOffloadObject::set_obj_attrs: gRPC SetObjectAttributes failed: {}", status.error_message());
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

  int ret = MDOFilterParentObject::set_obj_attrs(dpp, setattrs, delattrs, y);
  if (ret < 0) {
    // XXX uh-oh - what do we do here? We've already modified the remote.
    // XXX FIXME
    ldpp_dout(dpp, 20)
        << fmt::format(FMT_STRING("MDOffloadObject::set_obj_attrs: Filter*Object::set_obj_attrs() failed: {}"), ret)
        << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::set_obj_attrs: set_attrs() attrs={}"),
             MDOFilterParentObject::get_attrs())
      << dendl;
  return 0;
}

int MDOffloadObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj)
{
  // The Rados driver fetches the attributes from the backing store using this
  // call.

  // // Call the upstream driver first.
  // int ret = MDOFilterParentObject::get_obj_attrs(y, dpp, target_obj);
  // if (ret < 0) {
  //   COND_LOG_PFX(dpp, 20, "MDOffloadObject::get_obj_attrs: MDOFilter*Object::get_obj_attrs() failed: {}", ret);
  //   return ret;
  // }

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
    COND_LOG_PFX(dpp, 20, "MDOffloadObject::get_obj_attrs: gRPC GetObjectAttributes failed: {}",
        status.error_message());
    return -EINVAL; // XXX appropriate error code?
  }

  auto new_attrs = rgw::sal::Attrs {};
  for (const auto& kv : response.attributes()) {
    bufferlist bl;
    bl.append(kv.second);
    new_attrs[kv.first] = std::move(bl);
  }

  // Move the fetched attributes to the final destination.
  MDOFilterParentObject::set_attrs(std::move(new_attrs));
  COND_LOG_PFX(dpp, 20, "MDOffloadObject::get_obj_attrs: set_attrs() passthrough attrs={}", MDOFilterParentObject::get_attrs());

  return 0;
}
int MDOffloadObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp)
{
  // The Rados driver uses this to modify a single attribute. Note that an
  // attribute's use may be more than just a simple key/value pair; for
  // example, tags are all stored on the single attribute
  // RGW_ATTR_TAGS.

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
    COND_LOG_PFX(dpp, 20, "MDOffloadObject::modify_obj_attrs: gRPC SetObjectAttributes failed: {}", status.error_message());
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
  auto& attrs = MDOFilterParentObject::get_attrs();
  attrs[attr_name] = attr_val;
  // set_attrs(attrs);
  COND_LOG_PFX(dpp, 20, "MDOffloadObject::modify_obj_attrs: set_attrs() attrs={}", attrs);

  return 0;
}

int MDOffloadObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y)
{
  // The Rados driver uses this to delete a single attribute. It does it via
  // set_obj_attrs(), and we should do the same. Remember, all the tags are on
  // a single attribute RGW_ATTR_TAGS.

  // NOTE the Rados driver call to set_atomic() when modifying attributes. We
  // need to be VERY CAREFUL to not modify the upstream object's invariants;
  // changes are we may have to make that call here too, without the attr
  // changes.

  if (!MDOffloadObject::attr_is_exported(attr_name)) {
    // Passthrough with logging.
    COND_LOG_PFX(dpp, 20, "MDOffloadObject::delete_obj_attrs: non-exported attr_name='{}', passthrough", attr_name);
    return MDOFilterParentObject::delete_obj_attrs(dpp, attr_name, y);

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
      COND_LOG_PFX(dpp, 20, "MDOffloadObject::delete_obj_attrs: gRPC SetObjectAttributes failed: {}", status.error_message());
      return -ERR_INTERNAL_ERROR; // XXX appropriate error code?
    }

    // Only after gRPC success do we modify our cached attributes.
    Attrs rmattr;
    rmattr[attr_name] = bufferlist();

    COND_LOG_PFX(dpp, 20, "MDOffloadObject::delete_obj_attrs: attr_name={}", attr_name);
    return MDOFilterParentObject::set_obj_attrs(dpp, nullptr, &rmattr, y);
  }
}

Attrs& MDOffloadObject::get_attrs(void)
{
  // Passthrough.
  return MDOFilterParentObject::get_attrs();
}

const Attrs& MDOffloadObject::get_attrs(void) const
{
  // Passthrough.
  const auto& a = MDOFilterParentObject::get_attrs();
  if (get_bucket()) {
    COND_LOG_G(20, "MDOffloadObject::get_attrs (const): bucket={} bucket_id={} attrs={}",
        get_bucket()->get_name(), get_bucket()->get_bucket_id(), a);
  } else {
    COND_LOG_G(20, "MDOffloadObject::get_attrs (const): bucket=<null> attrs={}", a);
  }
  return a;
}

int MDOffloadObject::set_attrs(Attrs a)
{
  // Passthrough.
  return MDOFilterParentObject::set_attrs(std::move(a));
}

bool MDOffloadObject::has_attrs(void)
{
  // Passthrough.
  return MDOFilterParentObject::has_attrs();
}

void MDOffloadObject::set_bucket(Bucket* b)
{
  COND_LOG_G(20, "MDOffloadObject({})::set_bucket: bucket='{}' key={}",
      (void*)this, b ? b->get_name() : std::string("<null>"), get_key());
  rgw::sal::MDOFilterParentObject::set_bucket(b);
}

/****************************************************************************/

// MDOffloadMultipartUpload

int MDOffloadMultipartUpload::complete(const DoutPrefixProvider* dpp,
    optional_yield y, CephContext* cct,
    std::map<int, std::string>& part_etags,
    std::list<rgw_obj_index_key>& remove_objs,
    uint64_t& accounted_size, bool& compressed,
    RGWCompressionInfo& cs_info, off_t& ofs,
    std::string& tag, ACLOwner& owner,
    uint64_t olh_epoch,
    rgw::sal::Object* target_obj)
{

  // We have to call the upstream completer first, because amongst other
  // important things that completer calculates the ETag of the completed
  // object, which we need to export.
  //
  // This is especially frustrating as it means we have to edit the attributes
  // that are written to exclude 'export only' attributes.

  // XXX We call the upstream write completer first because we need the ETag.
  // This means we have to try really hard to write the metadata to the remote
  // - otherwise we're inconsistent. XXX

  // This *must* be a copy!
  auto original_attrs = target_obj->get_attrs();
  auto& target_attrs = target_obj->get_attrs();
  for (const auto& it : target_attrs) {
    if (MDOffloadObject::attr_import_prohibited(it.first)) {
      COND_LOG_PFX(dpp, 20, "MDOffloadMultipartUpload::complete: removing prohibited attr '{}' before complete()", it.first);
      target_attrs.erase(it.first);
    }
  }

  int ret = MDOFilterParentMultipartUpload::complete(dpp, y, cct, part_etags,
      remove_objs, accounted_size, compressed, cs_info, ofs,
      tag, owner, olh_epoch, target_obj);

  if (ret < 0) {
    LOG_PFX(dpp, 0, "MDOffloadMultipartUpload::complete: MDOFilterParentMultipartUpload::complete() failed ret={}", ret);
    return ret;
  }

  // For MultipartOffload subclasses, `bucket` is a protected field.
  auto key = target_obj->get_key();
  std::string log_prefix = fmt::format(FMT_STRING("MDOffloadMultipartUpload::complete: bucket={} bucket_id={} target_obj={} upload_id={}"),
      bucket->get_name(), bucket->get_bucket_id(), target_obj->get_key(), get_upload_id());

  if (!MDOffloadObject::has_attrs_required_to_create(target_obj->get_attrs())) {
    LOG_PFX(dpp, 0, "{} missing required attributes to create target_obj={}",
        log_prefix, target_obj->get_key());
    return -ERR_INTERNAL_ERROR; // XXX appropriate error code?
  }

  // XXX CHECK
  // The target_obj has the attributes we're interested in.
  COND_LOG_PFX(dpp, 20, "MDOffloadMultipartUpload::complete: {}, target_obj->attrs={}",
      log_prefix, target_obj->get_attrs());

  // Upload and then remove exportable attributes.
  auto client = driver_->channel()->create_client<gutil::MDOffloadGrpcClient>();
  mdoffload::v1::SetObjectAttributesRequest request;

  // Loop through the ORIGINAL attributes of the target object, and upload any
  // that are marked for export.
  // XXX auto& attrs = target_obj->get_attrs();
  int export_count = 0;
  // XXX relic of export-then-complete
  // std::vector<std::string> attrs_to_remove;

  for (const auto& it : original_attrs) {
    if (MDOffloadObject::attr_is_exported(it.first)) {
      COND_LOG_PFX(dpp, 20, "MDOffloadMultipartUpload::complete: setting exportable attr '{}' ({} bytes)'", it.first, it.second.length());
      (*request.mutable_attributes_to_add())[it.first] = it.second.to_str();
      export_count++;
    }
    // if (MDOffloadObject::attr_import_prohibited(it.first)) {
    //   attrs_to_remove.push_back(it.first);
    // }
  }
  if (export_count > 0) {
    ::grpc::ClientContext context;
    mdoffload::v1::SetObjectAttributesResponse response;
    request.set_bucket_name(bucket->get_name());
    request.set_bucket_id(bucket->get_bucket_id());
    request.set_object_key(key.name);
    request.set_object_instance_id(key.instance);
    request.set_new_object_instance(true); // Indicate this is a new object version.

    auto status = client->stub()->SetObjectAttributes(&context, request, &response);
    if (!status.ok()) {
      LOG_PFX(dpp, 0, "MDOffloadMultipartUpload::complete: gRPC SetObjectAttributes failed: {}", status.error_message());
      return -ERR_INTERNAL_ERROR; // XXX appropriate error code?
      // XXX XXX THIS IS BAD - we've already completed the upload!
    }
  } else {
    COND_LOG_PFX(dpp, 20, "MDOffloadMultipartUpload::complete: no exportable attributes to set");
  }
  // XXX relic of export-then-complete
  // if (!attrs_to_remove.empty()) {
  //   for (const auto& attr_name : attrs_to_remove) {
  //     COND_LOG_PFX(dpp, 20, "MDOffloadMultipartUpload::complete: removing prohibited attr '{}'", attr_name);
  //     attrs.erase(attr_name);
  //   }
  // }

  return 0;
}

/****************************************************************************/

// MDOffloadWriter

int MDOffloadWriter::prepare(optional_yield y)
{
  COND_LOG_G(20, "MDOffloadWriter::prepare: object='{}'",
      obj ? obj->get_name() : std::string("<null>"));
  return MDOFilterParentWriter::prepare(y);
}

int MDOffloadWriter::process(bufferlist&& data, uint64_t offset)
{
  COND_LOG_G(20, "MDOffloadWriter::process: object='{}' offset={} len={}",
      obj ? obj->get_name() : std::string("<null>"), offset, data.length());
  return MDOFilterParentWriter::process(std::move(data), offset);
}

int MDOffloadWriter::complete(size_t accounted_size, const std::string& etag,
    ceph::real_time* mtime, ceph::real_time set_mtime,
    std::map<std::string, bufferlist>& attrs, ceph::real_time delete_at,
    const char* if_match, const char* if_nomatch, const std::string* user_data,
    rgw_zone_set* zones_trace, bool* canceled, optional_yield y, uint32_t flags)
{

  // XXX for consistency with MultipartUpload::complete(), we call the
  // upstream write completer first. This means we have to try really hard to
  // write the metadata to the remote - otherwise we're inconsistent. XXX

  COND_LOG_G(20, "MDOffloadWriter::complete: object='{}' accounted_size={} flags={} attrs={}",
      obj ? obj->get_name() : std::string("<null>"), accounted_size, flags, attrs);

  if (!MDOffloadObject::has_attrs_required_to_create(attrs)) {
    LOG_G(0, "MDOffloadWriter::complete: missing required attributes to create object='{}'",
        obj ? obj->get_name() : std::string("<null>"));
    return -ERR_INTERNAL_ERROR; // XXX appropriate error code?
  }

  // Create a filtered attribute set for RADOS, removing any external-only attributes.
  Attrs filtered_attrs;
  for (const auto& it : attrs) {
    if (!MDOffloadObject::attr_import_prohibited(it.first)) {
      filtered_attrs[it.first] = it.second;
    } else {
      COND_LOG_G(20, "MDOffloadWriter::complete: removing external-only attribute '{}' from attrs for object='{}'",
          it.first, obj ? obj->get_name() : std::string("<null>"));
    }
  }

  int ret = MDOFilterParentWriter::complete(accounted_size, etag, mtime, set_mtime, filtered_attrs,
      delete_at, if_match, if_nomatch, user_data, zones_trace, canceled, y,
      flags);
  if (ret < 0) {
    LOG_G(0, "MDOffloadWriter::complete: MDOFilter*Writer::complete() failed ret={}", ret);
    return ret;
  }

  // Send the full set of attributes to the remote store.
  auto object = obj;
  if (!object) {
    // XXX Can this really happen?
    LOG_G(0, "MDOffloadWriter::complete: no object");
    return -EINVAL; // XXX appropriate error code?
  }
  auto bucket = obj->get_bucket();
  if (!bucket) {
    LOG_G(0, "MDOffloadWriter::complete: no bucket for object='{}'",
        obj ? obj->get_name() : std::string("<null>"));
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
      COND_LOG_G(20, "{}: setting attr '{}' ({} bytes)", msg_prefix, it.first, it.second.length());
      (*request.mutable_attributes_to_add())[it.first] = it.second.to_str();
      attr_count++;
    } else {
      COND_LOG_G(20, "MDOffloadWriter::complete: skipping non-exported attr '{}'", it.first);
    }
  }
  if (attr_count == 0) {
    COND_LOG_G(20, "{}: no exported attributes to set for object='{}'", msg_prefix, object->get_name());
  } else {

    request.set_bucket_name(bucket->get_name());
    request.set_bucket_id(bucket->get_bucket_id());
    request.set_object_key(object->get_key().name);
    request.set_object_instance_id(object->get_key().instance);
    request.set_new_object_instance(true); // Indicate this is a new object version.

    auto status = client->stub()->SetObjectAttributes(&context, request, &response);
    if (!status.ok()) {
      LOG_G(0, "{}: gRPC SetObjectAttributes failed: {}", msg_prefix, status.error_message());
      return -ERR_INTERNAL_ERROR; // XXX appropriate error code?
                                  // XXX XXX THIS IS BAD - we've already completed the upload!
    }
  }

  // ldout(g_ceph_context, 20)
  //     << fmt::format(FMT_STRING("MDOffloadWriter::complete: object='{}' WRITE EMPTY ATTRS "),
  //            object->get_name(), attrs)
  //     << dendl;
  // Attrs empty_attrs;
  // return MDOFilterParentWriter::complete(accounted_size, etag, mtime, set_mtime, empty_attrs,
  //     delete_at, if_match, if_nomatch, user_data, zones_trace, canceled, y,
  //     flags);

  // ldout(g_ceph_context, 20)
  //     << fmt::format(FMT_STRING("MDOffloadWriter::complete: object='{}' WRITE COMPLETE ATTRS "),
  //            object->get_name(), attrs)
  //     << dendl;
  // return MDOFilterParentWriter::complete(accounted_size, etag, mtime, set_mtime, attrs,
  //     delete_at, if_match, if_nomatch, user_data, zones_trace, canceled, y,
  //     flags);

  // XXX relic from the 'export then update' period.
  // for (const auto& it : attrs) {
  //   if (MDOffloadObject::attr_import_prohibited(it.first)) {
  //     LOG_G(1, "{}: removing external-only attribute '{}' from local attrs", msg_prefix, it.first);
  //     attrs.erase(it.first);
  //   }
  // }
  return 0;
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
};

bool MDOffloadObject::attr_needs_import_check(const std::string& attr_name)
{

  return attr_object_import_check.contains(attr_name);
}

// Attributes we don't want stored in Rados. This means we intercept them in
// Writer::prepare() and remove them from the Attrs passed to the next driver.
static std::set<std::string> attr_object_import_prohibited = {
  RGW_ATTR_ACL,
  RGW_ATTR_CRYPT_CONTEXT,
  RGW_ATTR_CRYPT_DATAKEY,
  RGW_ATTR_CRYPT_KEYID,
  RGW_ATTR_CRYPT_KEYMD5,
  RGW_ATTR_CRYPT_KEYSEL,
  RGW_ATTR_CRYPT_MODE,
  RGW_ATTR_CRYPT_PARTS,
  RGW_ATTR_CRYPT_PREFIX,
  RGW_ATTR_TAGS,
};

bool MDOffloadObject::attr_import_prohibited(const std::string& attr_name)
{
  return attr_object_import_prohibited.contains(attr_name);
}

static std::set<std::string> attr_object_required_to_create = {
  RGW_ATTR_ETAG,
};

bool MDOffloadObject::has_attrs_required_to_create(const Attrs& attrs)
{
  for (const auto& attr_name : attr_object_required_to_create) {
    if (attrs.find(attr_name) == attrs.end()) {
      COND_LOG_G(20, "MDOffloadObject::has_attrs_required_to_create: missing required attr '{}'", attr_name);
      return false;
    }
  }
  return true;
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
