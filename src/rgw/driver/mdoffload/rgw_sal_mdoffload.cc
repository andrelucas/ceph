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
#include "common/dout.h"
#include "global/global_context.h"
#include "mdoffload/v1/mdoffload.pb.h"
#include "rgw_common.h"
#include "rgw_sal.h"

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
      << fmt::format(FMT_STRING("MDOffloadFilterDriver::get_object: rgw_obj_key k={}"), k)
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
  fb->set_attrs(akamai::grpcutil::attrs_from_proto(response.attributes()));
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
  auto Attrs = akamai::grpcutil::attrs_from_proto(response.attributes());
  Bucket* fb = new MDOffloadBucket(std::move(nb), u, this, std::move(Attrs));
  bucket->reset(fb);

  return 0;
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
  std::unique_ptr<Object> new_object = next->get_object(key);
  if (!new_object)
    return nullptr;

  // Wrap the Object in an MDOffloadObject.
  auto md_object = std::make_unique<MDOffloadObject>(std::move(new_object), this, driver_);
  return md_object;
}

/****************************************************************************/

// rgw::sal::MDOffloadObject

// rgw::sal::MDOffloadObject::MDOffloadReadOp

std::unique_ptr<Object::ReadOp> MDOffloadObject::get_read_op()
{
  // Almost-duplicate of FilterObject::get_read_op() returning the correct
  // type.
  std::unique_ptr<ReadOp> r = next->get_read_op();
  return std::make_unique<MDOffloadReadOp>(std::move(r), get_bucket(), driver_);
}

int MDOffloadObject::MDOffloadReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  // Passthrough with logging.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::prepare"))
      << dendl;
  return FilterObject::FilterReadOp::prepare(y, dpp);
}

int MDOffloadObject::MDOffloadReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
    optional_yield y, const DoutPrefixProvider* dpp)
{
  // Passthrough with logging.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::read: ofs={} end={}"), ofs, end)
      << dendl;
  return FilterObject::FilterReadOp::read(ofs, end, bl, y, dpp);
}

int MDOffloadObject::MDOffloadReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  // Passthrough with logging.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::get_attr: name='{}'"), name)
      << dendl;
  return FilterObject::FilterReadOp::get_attr(dpp, name, dest, y);
}

int MDOffloadObject::MDOffloadReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
    int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  // Passthrough with logging.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadReadOp::iterate: ofs={} end={}"), ofs, end)
      << dendl;
  return FilterObject::FilterReadOp::iterate(dpp, ofs, end, cb, y);
}

// rgw::sal::MDOffloadObject::MDOffloadDeleteOp

std::unique_ptr<Object::DeleteOp> MDOffloadObject::get_delete_op()
{
  // Almost-duplicate of FilterObject::get_delete_op() returning the correct
  // type.
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<MDOffloadDeleteOp>(std::move(d), get_bucket(), driver_);
}

int MDOffloadObject::MDOffloadDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags)
{
  // XXX placeholder: delete upstream attributes, or at least mark as
  // deleting.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::MDOffloadDeleteOp::delete_obj: flags={}"), flags)
      << dendl;
  int r = FilterObject::FilterDeleteOp::delete_obj(dpp, y, flags);
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

  // Only after gRPC success do we modify our cached attributes.
  Attrs new_attrs = cached_attrs_;
  if (setattrs != nullptr) {
    for (const auto& it : *setattrs) {
      new_attrs[it.first] = it.second;
    }
  }
  if (delattrs != nullptr) {
    for (const auto& it : *delattrs) {
      new_attrs.erase(it.first);
    }
  }
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::set_obj_attrs: setattrs={} delattrs={} cached_attrs_={}"),
             fmt_maybe(setattrs),
             fmt_maybe(delattrs),
             cached_attrs_)
      << dendl;
  cached_attrs_ = new_attrs;
  has_attrs_ = true;

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

  cached_attrs_ = std::move(new_attrs);
  has_attrs_ = true;
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::get_obj_attrs: cached_attrs_={}"),
             cached_attrs_)
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

  // Only after gRPC success do we modify our cached attributes.
  Attrs new_attrs = cached_attrs_;
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::modify_obj_attrs: attr_name={} attr_val[{} bytes]"),
             attr_name, attr_val.length())
      << dendl;
  new_attrs[attr_name] = attr_val;
  cached_attrs_ = new_attrs;

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
    ldpp_dout(dpp, 20) << fmt::format(FMT_STRING("MDOffloadObject::delete_obj_attrs: gRPC SetObjectAttributes failed: {}"),
        status.error_message())
                       << dendl;
    return -EINVAL; // XXX appropriate error code?
  }

  // Only after gRPC success do we modify our cached attributes.
  Attrs rmattr;
  rmattr[attr_name] = bufferlist();
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadObject::delete_obj_attrs: attr_name={}"),
             attr_name)
      << dendl;
  return set_obj_attrs(dpp, nullptr, &rmattr, y);
}

Attrs& MDOffloadObject::get_attrs(void)
{
  // XXX placeholder (but probably not far wrong).
  return cached_attrs_;
}

const Attrs& MDOffloadObject::get_attrs(void) const
{
  // XXX placeholder (but probably not far wrong).
  return cached_attrs_;
}

int MDOffloadObject::set_attrs(Attrs a)
{
  // XXX placeholder (but probably not far wrong).
  cached_attrs_ = a;
  return 0;
}

bool MDOffloadObject::has_attrs(void)
{
  // XXX placeholder (but probably not far wrong).
  return has_attrs_;
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
