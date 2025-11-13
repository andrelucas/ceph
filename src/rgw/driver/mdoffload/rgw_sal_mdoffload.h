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
#include <sstream>
#include <stdexcept>
#include <string>

#include <mutex>

#include <fmt/format.h>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>

#include "mdoffload/v1/mdoffload.grpc.pb.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_sal_filter.h"

// fmtlib formatters for Ceph types. Some of these have to_str() and their own
// operator<<, but this way keeps things consistent.

template <>
struct fmt::formatter<rgw_user> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const rgw_user& u, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    return fmt::format_to(ctx.out(), "rgw_user{{tenant='{}',id='{}',ns='{}'}}",
        u.tenant, u.id, u.ns);
  }
};

template <>
struct fmt::formatter<rgw_bucket> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const rgw_bucket& b, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    return fmt::format_to(ctx.out(), "rgw_bucket{{tenant='{}',name='{}',id='{}'}}",
        b.tenant, b.name, b.bucket_id);
  }
};

template <>
struct fmt::formatter<rgw::sal::User> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  // Annoyingly we can't use const rgw::sal::User& as some of the accessors
  // aren't marked as const.
  template <typename FormatContext>
  auto format(rgw::sal::User& u, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    // No point showing the id, it's just a composite of the fields we're
    // already showing.
    return fmt::format_to(ctx.out(), FMT_STRING("rgw::sal::User{{tenant='{}',name='{}',ns='{}'}}"),
        u.get_tenant(), u.get_display_name(), u.get_ns());
  }
};

template <>
struct fmt::formatter<RGWBucketInfo> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const RGWBucketInfo& info, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    const auto placement = info.placement_rule.empty() ? std::string { "<unset>" } : info.placement_rule.to_str();
    const auto sync_state = info.sync_policy ? "set" : "unset";

    return fmt::format_to(
        ctx.out(),
        FMT_STRING("RGWBucketInfo{{bucket={},owner={},zonegroup='{}',placement='{}',flags=0x{:x},versioned={},swift_versioning={},obj_lock_enabled={},requester_pays={},has_website={},mdsearch_fields={},sync_policy={}}}"),
        info.bucket,
        info.owner,
        info.zonegroup,
        placement,
        info.flags,
        info.versioned(),
        info.has_swift_versioning(),
        info.obj_lock_enabled(),
        info.requester_pays,
        info.has_website,
        info.mdsearch_config.size(),
        sync_state);
  }
};

template <>
struct fmt::formatter<rgw::sal::Attrs> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const rgw::sal::Attrs& attrs, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    auto out = ctx.out();
    out = fmt::format_to(out, "rgw::sal::Attrs{{");

    bool first = true;
    for (const auto& [key, value] : attrs) {
      if (!first) {
        out = fmt::format_to(out, ", ");
      }
      first = false;

      // XXX TO BE CONTINUED: We should read the key name and decode the
      // values accordingly, for the keys we care about. For now, just use
      // to_str() and accept the carnage.
      out = fmt::format_to(out, FMT_STRING("'{}':{}"), key, value.to_str());
    }

    out = fmt::format_to(out, "}}");
    return out;
  }
};

/**
 * @brief Safely format a type at the end of a potentially-null pointer.
 *
 * Utility function for logging using fmtlib. Relies on type \p T having a
 * formatter.
 *
 * ```C++
 * // Safely format a potentially-null pointer to rgw_user.
 * rgw_user* u = ...;
 * ldpp_dout(dpp, 20) << fmt::format(FMT_STRING("User u={}"), fmt_maybe(u)) << dendl;
 * ```
 *
 * @tparam T
 * @param t
 * @return std::string
 */
template <typename T>
std::string fmt_maybe(T* t)
{
  if (t)
    return fmt::format(FMT_STRING("{}"), *t);
  else
    return "NULL";
}

namespace akamai::grpcutil {

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

namespace gutil = akamai::grpcutil;

class MDOffloadFilterDriver : public FilterDriver {

private:
  std::shared_ptr<gutil::GrpcChannelWrapper> channelwrapper_;

public:
  MDOffloadFilterDriver(CephContext* cct, rgw::sal::Driver* next)
      : FilterDriver(next)
  {
  }
  virtual ~MDOffloadFilterDriver() = default;

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

  // Non-inherited methods.
  std::shared_ptr<gutil::GrpcChannelWrapper> channel() { return channelwrapper_; }

public:
  class BadNextDriver : public std::runtime_error {
    using std::runtime_error::runtime_error; // Inherit constructors.
  };

}; // class MDOffloadDriver

class MDOffloadUser : public FilterUser {
private:
  MDOffloadFilterDriver* driver_ = nullptr;

public:
  MDOffloadUser(std::unique_ptr<User> next, MDOffloadFilterDriver* driver)
      : FilterUser(std::move(next))
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

class MDOffloadBucket : public FilterBucket {

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
  rgw::sal::Attrs cached_attrs_;

public:
  MDOffloadBucket(std::unique_ptr<Bucket> next, User* user, MDOffloadFilterDriver* driver)
      : FilterBucket(std::move(next), user)
      , driver_(driver)
  {
  }
  virtual ~MDOffloadBucket() = default;

  virtual Attrs& get_attrs() override;
  virtual int set_attrs(Attrs a) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) override;

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;

}; // class MDOffloadFilterBucket

class MDOffloadObject : public FilterObject {

private:
  MDOffloadFilterDriver* driver_ = nullptr;

  /**
   * @brief Cached object attributes.
   *
   * As with MDOffloadBucket::cached_attrs_, it's tempting to make this more
   * complicated, but the API is constructed around returning a reference to
   * this field (via get_attrs()) so we need to keep it simple. If we do
   * complicate things, we need a way to still return that simple reference so
   * we're not having to change the API in multiple places, creating a
   * maintenance problem over time.
   */
  rgw::sal::Attrs cached_attrs_;
  bool has_attrs_ = false;

public:
  MDOffloadObject(std::unique_ptr<Object> next, Bucket* bucket, MDOffloadFilterDriver* driver)
      : FilterObject(std::move(next), bucket)
      , driver_(driver)
  {
  }
  virtual ~MDOffloadObject() = default;

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

}; // class MDOffloadObject

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
