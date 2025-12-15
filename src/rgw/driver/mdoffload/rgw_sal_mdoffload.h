/**
 * @file rgw_sal_mdoffload.h
 * @author André Lucas (alucas@akamai.com)
 * @brief SAL Metadata Offload Driver - Header file
 * @version 0.1
 * @date 2025-09-23
 *
 * @copyright Copyright (c) 2025
 *
 */

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <mutex>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>

#include "mdoffload/v1/mdoffload.grpc.pb.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_sal_filter.h"
#include "rgw_sal_filterlog.h"

/*****************************************************************************/

// Define this to 1 to have the MDOffload driver inherit from FilterLogDriver,
// undefine it to have it inherit from FilterDriver directly.
#define RGW_MDOFFLOAD_LOGGING_FILTER_DRIVER 1
// #undef RGW_MDOFFLOAD_LOGGING_FILTER_DRIVER

/*****************************************************************************/

namespace akamai::grpcutil {

rgw::sal::Attrs attrs_from_proto(const ::google::protobuf::Map<std::string, std::string>& proto_attrs);

class MDOffloadGrpcClient {
private:
  std::shared_ptr<mdoffload::v1::MDOffloadService::Stub> stub_;

public:
  explicit MDOffloadGrpcClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(mdoffload::v1::MDOffloadService::NewStub(channel))
  {
  }

  ~MDOffloadGrpcClient() = default;

  std::shared_ptr<mdoffload::v1::MDOffloadService::Stub> stub()
  {
    return stub_;
  }
}; // class MDOffloadGrpcClient

/**
 * @brief gRPC Channel Wrapper
 *
 * This class provides a thread-safe wrapper around a gRPC channel.
 *
 * Currently very, very simple. We will add support for channel parameters
 * and mTLS as we go. This is intended for reuse.
 */
class GrpcChannelWrapper {
private:
  using mutex_type = std::mutex;

  mutable mutex_type mutex_;
  std::shared_ptr<::grpc::Channel> channel_;
  std::string uri_;

public:
  GrpcChannelWrapper(const std::string& uri)
      : uri_(uri)
  {
    _set_channel();
  }
  ~GrpcChannelWrapper() { }

  /// Return a shared_ptr to the gRPC channel.
  std::shared_ptr<::grpc::Channel> channel()
  {
    std::lock_guard<mutex_type> lock(mutex_);
    return channel_;
  }

  /// Return a new client instance for the wrapped channel. All the client
  /// type needs is a constructor taking a shared_ptr<grpc::Channel>.
  template <typename T>
  std::unique_ptr<T> create_client()
  {
    return std::make_unique<T>(channel());
  }

  /// Return the configured channel uri.
  const std::string& channel_uri() const
  {
    std::lock_guard<mutex_type> lock(mutex_);
    return uri_;
  }

  /** @brief Set a new channel URI, recreating the channel.
   *
   * grpc::CreateChannel() is lazy, it doesn't actually connect until the
   * first RPC is made, so this operation can't (currently) fail.
   *
   * @param uri New URI
   * @return true The operation succeeded, false otherwise.
   */
  bool set_channel_uri(const std::string& uri)
  {
    std::lock_guard<mutex_type> lock(mutex_);
    uri_ = uri;
    _set_channel();
    return true;
  }

  // Internal: create the channel. Assumes we're constructing or holding the
  // mutex, don't take the lock here.
  bool _set_channel()
  {
    channel_ = ::grpc::CreateChannel(uri_, ::grpc::InsecureChannelCredentials()); // XXX mTLS
    return true;
  }

}; // class GrpcChannelWrapper

}; // namespace akamai::grpcutil

namespace rgw::sal {

// Make it a bit easier to switch between FilterDriver and
// FilterLogDriver as base classes for our MDOffload* classes.

#ifdef RGW_MDOFFLOAD_LOGGING_FILTER_DRIVER

using MDOFilterParentDriver = FilterLogDriver;
using MDOFilterParentUser = FilterLogUser;
using MDOFilterParentBucket = FilterLogBucket;
using MDOFilterParentObject = FilterLogObject;
using MDOFilterParentMultipartUpload = FilterLogMultipartUpload;
using MDOFilterParentMultipartPart = FilterLogMultipartPart;
using MDOFilterParentWriter = FilterLogWriter;
using MDOFilterParentObjectReadOp = FilterLogObject::FilterLogReadOp;
using MDOFilterParentObjectDeleteOp = FilterLogObject::FilterLogDeleteOp;

#else // !RGW_MDOFFLOAD_LOGGING_FILTER_DRIVER

using MDOFilterParentDriver = FilterDriver;
using MDOFilterParentUser = FilterUser;
using MDOFilterParentBucket = FilterBucket;
using MDOFilterParentObject = FilterObject;
using MDOFilterParentMultipartUpload = FilterMultipartUpload;
using MDOFilterParentMultipartPart = FilterMultipartPart;
using MDOFilterParentWriter = FilterWriter;
using MDOFilterParentObjectReadOp = FilterObject::FilterReadOp;
using MDOFilterParentObjectDeleteOp = FilterObject::FilterDeleteOp;

#endif // RGW_MDOFFLOAD_LOGGING_FILTER_DRIVER

namespace gutil = akamai::grpcutil;

class MDOffloadFilterDriver : public FilterLogDriver {

private:
  std::shared_ptr<gutil::GrpcChannelWrapper> channelwrapper_;

public:
  MDOffloadFilterDriver(CephContext* cct, rgw::sal::Driver* next)
      : FilterLogDriver(next)
  {
  }
  virtual ~MDOffloadFilterDriver() = default;

  // Delete copy and move constructors and assignment operators. ATTOW they're
  // not used, let's not assume that will always be the case.
  MDOffloadFilterDriver(const MDOffloadFilterDriver&) = delete;
  MDOffloadFilterDriver& operator=(const MDOffloadFilterDriver&) = delete;
  MDOffloadFilterDriver(MDOffloadFilterDriver&&) = delete;
  MDOffloadFilterDriver& operator=(MDOffloadFilterDriver&&) = delete;

  virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp) override;

  virtual const std::string get_name() const override;

  // We have to override get_user*() to return our MDOffloadUser instead of FilterUser.

  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
  virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y,
      std::unique_ptr<User>* user) override;
  virtual int get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y,
      std::unique_ptr<User>* user) override;
  virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y,
      std::unique_ptr<User>* user) override;

  // This is called by RGWHandler_REST_S3::init_from_header() to initialise an
  // object with no bucket reference. It still needs to return the right type
  // of object, but we need to be careful as there's no bucket so we can't do
  // any lookups on it. I think it's really just a container for the key info
  // tbh.
  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;

  // We have to override get_bucket(*) to return our MDOffloadBucket instead of FilterBucket.

  /** Get a Bucket by info.  Does not query the driver, just uses the give bucket info. */
  virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) override;
  /** Lookup a Bucket by key.  Queries driver for bucket info. */
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
  /** Lookup a Bucket by name.  Queries driver for bucket info. */
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y) override;

  virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider* dpp,
      optional_yield y,
      rgw::sal::Object* obj,
      const rgw_user& owner,
      const rgw_placement_rule* ptail_placement_rule,
      const std::string& unique_tag,
      uint64_t position,
      uint64_t* cur_accounted_size) override;

  virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider* dpp,
      optional_yield y,
      rgw::sal::Object* obj,
      const rgw_user& owner,
      const rgw_placement_rule* ptail_placement_rule,
      uint64_t olh_epoch,
      const std::string& unique_tag) override;

  // Non-inherited methods.
  std::shared_ptr<gutil::GrpcChannelWrapper> channel() { return channelwrapper_; }

public:
  class BadNextDriver : public std::runtime_error {
    using std::runtime_error::runtime_error; // Inherit constructors.
  };

}; // class MDOffloadDriver

class MDOffloadUser : public MDOFilterParentUser {
private:
  MDOffloadFilterDriver* driver_ = nullptr;

public:
  MDOffloadUser(std::unique_ptr<User> next, MDOffloadFilterDriver* driver)
      : MDOFilterParentUser(std::move(next))
      , driver_(driver)
  {
  }
  virtual ~MDOffloadUser() = default;

  // create_bucket() must be overriden because it's the only place that calls
  // set_attrs() on the parent RadosBucket. FilterBucket::set_attrs() is never
  // called, at least not in v18.2.7.

  virtual int create_bucket(const DoutPrefixProvider* dpp,
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
      std::unique_ptr<Bucket>* bucket,
      optional_yield y) override;
}; // class MDOffloadUser

class MDOffloadBucket : public MDOFilterParentBucket {

private:
  MDOffloadFilterDriver* driver_ = nullptr;

  /**
   * @brief Cached bucket attributes.
   *
   * It's very tempting to make this a more complicated data structure with
   * locking etc., but the API is constructed around returning a reference to
   * this field (via get_attrs()) so we need to keep it simple. If we do
   * complicate things, we need a way to still return that simple reference so
   * we're not having to change the API in multiple places, creating a
   * maintenance problem over time.
   */
  ::rgw::sal::Attrs cached_attrs_;

public:
  MDOffloadBucket(std::unique_ptr<Bucket> next, User* user, MDOffloadFilterDriver* driver, Attrs attrs = {})
      : MDOFilterParentBucket(std::move(next), user)
      , driver_(driver)
      , cached_attrs_(std::move(attrs))
  {
  }
  virtual ~MDOffloadBucket() = default;

  // Delete copy and move constructors and assignment operators. ATTOW they're
  // not used, let's not assume that will always be the case.
  MDOffloadBucket(const MDOffloadBucket&) = delete;
  MDOffloadBucket& operator=(const MDOffloadBucket&) = delete;
  MDOffloadBucket(MDOffloadBucket&&) = delete;
  MDOffloadBucket& operator=(MDOffloadBucket&&) = delete;

  virtual Attrs& get_attrs() override;
  virtual int set_attrs(Attrs a) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) override;

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;

  virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
      const std::string& oid,
      std::optional<std::string> upload_id = std::nullopt,
      ACLOwner owner = {}, ceph::real_time mtime = real_clock::now()) override;
  virtual int list_multiparts(const DoutPrefixProvider* dpp,
      const std::string& prefix,
      std::string& marker,
      const std::string& delim,
      const int& max_uploads,
      std::vector<std::unique_ptr<MultipartUpload>>& uploads,
      std::map<std::string, bool>* common_prefixes,
      bool* is_truncated) override;
  virtual int abort_multiparts(const DoutPrefixProvider* dpp,
      CephContext* cct) override;

}; // class MDOffloadFilterBucket

class MDOffloadObject : public MDOFilterParentObject {

private:
  MDOffloadFilterDriver* driver_ = nullptr;

  // /**
  //  * @brief Cached object attributes.
  //  *
  //  * As with MDOffloadBucket::cached_attrs_, it's tempting to make this more
  //  * complicated, but the API is constructed around returning a reference to
  //  * this field (via get_attrs()) so we need to keep it simple. If we do
  //  * complicate things, we need a way to still return that simple reference so
  //  * we're not having to change the API in multiple places, creating a
  //  * maintenance problem over time.
  //  */
  // rgw::sal::Attrs cached_attrs_;
  // bool has_attrs_ = false;

public:
  MDOffloadObject(std::unique_ptr<Object> next, MDOffloadFilterDriver* driver);
  MDOffloadObject(std::unique_ptr<Object> next, Bucket* bucket, MDOffloadFilterDriver* driver);
  // Clone 'constructor'.
  MDOffloadObject(MDOffloadObject& _o);
  virtual ~MDOffloadObject() = default;

  // Delete copy and move constructors and assignment operators. ATTOW they're
  // not used, let's not assume that will always be the case.
  MDOffloadObject(const MDOffloadObject&) = delete;
  MDOffloadObject& operator=(const MDOffloadObject&) = delete;
  MDOffloadObject(MDOffloadObject&&) = delete;
  MDOffloadObject& operator=(MDOffloadObject&&) = delete;

  struct MDOffloadReadOp : MDOFilterParentObjectReadOp {
    std::unique_ptr<ReadOp> next;
    Bucket* bucket_;
    Object* object_;
    MDOffloadFilterDriver* driver_;

    MDOffloadReadOp(std::unique_ptr<ReadOp> _next, Object* object, Bucket* bucket, MDOffloadFilterDriver* driver)
        : MDOFilterParentObjectReadOp(std::move(_next))
        , bucket_(bucket)
        , object_(object)
        , driver_(driver)
    {
    }
    virtual ~MDOffloadReadOp() = default;

    virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y,
        const DoutPrefixProvider* dpp) override;
    virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
        RGWGetDataCB* cb, optional_yield y) override;
    virtual int get_attr(const DoutPrefixProvider* dpp, const char* name,
        bufferlist& dest, optional_yield y) override;
  };

  struct MDOffloadDeleteOp : MDOFilterParentObjectDeleteOp {
    std::unique_ptr<DeleteOp> next;
    Bucket* bucket_;
    Object* object_;
    MDOffloadFilterDriver* driver_;

    MDOffloadDeleteOp(std::unique_ptr<DeleteOp> _next, Object* object, Bucket* bucket, MDOffloadFilterDriver* driver)
        : MDOFilterParentObjectDeleteOp(std::move(_next))
        , bucket_(bucket)
        , object_(object)
        , driver_(driver)
    {
    }

    virtual ~MDOffloadDeleteOp() = default;

    virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;
  };

  virtual int delete_object(const DoutPrefixProvider* dpp,
      optional_yield y,
      uint32_t flags) override;
  virtual int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
      bool keep_index_consistent, optional_yield y) override;

  /** Set attributes for this object from the backing store.  Attrs can be set or
   * deleted.  @note the attribute APIs may be revisited in the future. */
  virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y) override;
  /** Get attributes for this object */
  virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) override;
  /** Modify attributes for this object. */
  virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) override;
  /** Delete attributes for this object */
  virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y) override;

  /** Get the cached attributes for this object */
  virtual Attrs& get_attrs(void) override;
  /** Get the (const) cached attributes for this object */
  virtual const Attrs& get_attrs(void) const override;
  /** Set the cached attributes for this object */
  virtual int set_attrs(Attrs a) override;
  /** Check to see if attributes are cached on this object */
  virtual bool has_attrs(void) override;

  virtual void set_bucket(Bucket* b) override;

  virtual std::unique_ptr<ReadOp> get_read_op() override;
  virtual std::unique_ptr<DeleteOp> get_delete_op() override;

  // Utilities.
public:
  // Attributes that are exported from RADOS to offload.
  static bool attr_is_exported(const std::string& attr_name);
  // Attributes that need to be checked when exporting from RADOS to offload.
  // If they differ, there's a problem.
  static bool attr_needs_import_check(const std::string& attr_name);
  // Don't allow certain attributes to be imported from offload to RADOS.
  static bool attr_import_prohibited(const std::string& attr_name);

}; // class MDOffloadObject

class MDOffloadMultipartUpload : public MDOFilterParentMultipartUpload {
private:
  MDOffloadFilterDriver* driver_ { nullptr };

public:
  MDOffloadMultipartUpload(std::unique_ptr<MultipartUpload> next,
      Bucket* bucket, Driver* driver)
      : MDOFilterParentMultipartUpload(std::move(next), bucket)
      , driver_(static_cast<MDOffloadFilterDriver*>(driver))
  {
  }
  virtual ~MDOffloadMultipartUpload() override = default;

  virtual int complete(const DoutPrefixProvider* dpp,
      optional_yield y, CephContext* cct,
      std::map<int, std::string>& part_etags,
      std::list<rgw_obj_index_key>& remove_objs,
      uint64_t& accounted_size, bool& compressed,
      RGWCompressionInfo& cs_info, off_t& ofs,
      std::string& tag, ACLOwner& owner,
      uint64_t olh_epoch,
      rgw::sal::Object* target_obj) override;
}; // class MDOffloadMultipartUpload

class MDOffloadWriter : public MDOFilterParentWriter {
private:
  MDOffloadFilterDriver* driver_ { nullptr };

public:
  MDOffloadWriter(std::unique_ptr<Writer> next, Object* obj, MDOffloadFilterDriver* driver)
      : MDOFilterParentWriter(std::move(next), obj)
      , driver_(driver)
  {
  }
  ~MDOffloadWriter() override = default;

  int prepare(optional_yield y) override;
  int process(bufferlist&& data, uint64_t offset) override;
  int complete(size_t accounted_size, const std::string& etag,
      ceph::real_time* mtime, ceph::real_time set_mtime,
      std::map<std::string, bufferlist>& attrs,
      ceph::real_time delete_at, const char* if_match,
      const char* if_nomatch, const std::string* user_data,
      rgw_zone_set* zones_trace, bool* canceled,
      optional_yield y, uint32_t flags) override;
};

} // namespace rgw::sal

/**
 * @brief Initialise a fresh MDOffloadFilter driver.
 *
 * @details This function is expected to be called by
 * DriverManager::init_storage_provider() and
 * DriverManager::init_raw_storage_provider(), and is used to create a new
 * filter. The returned driver must still be initialized by the caller via
 * initialize().
 *
 * @param cct The context.
 * @param next The next driver in the chain, normally RADOS.
 * @return rgw::sal::Driver*
 */
rgw::sal::Driver* newMDOffloadFilter(CephContext* cct, rgw::sal::Driver* next);
