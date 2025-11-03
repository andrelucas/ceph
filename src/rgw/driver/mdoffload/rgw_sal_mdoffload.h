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

#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_sal_filter.h"
#include <stdexcept>

namespace rgw::sal {

class MDOffloadFilterDriver : public FilterDriver {
public:
  MDOffloadFilterDriver(CephContext* cct, rgw::sal::Driver* next)
      : FilterDriver(next)
  {
  }
  virtual ~MDOffloadFilterDriver() = default;

  virtual const std::string get_name() const override;

  // We have to override get_user*() to return our MDOffloadUser instead of FilterUser.

  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
  virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y,
      std::unique_ptr<User>* user) override;
  virtual int get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y,
      std::unique_ptr<User>* user) override;
  virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y,
      std::unique_ptr<User>* user) override;

  // We have to override get_bucket(*) to return our MDOffloadBucket instead of FilterBucket.

  /** Get a Bucket by info.  Does not query the driver, just uses the give bucket info. */
  virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) override;
  /** Lookup a Bucket by key.  Queries driver for bucket info. */
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
  /** Lookup a Bucket by name.  Queries driver for bucket info. */
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y) override;

public:
  class BadNextDriver : public std::runtime_error {
    using std::runtime_error::runtime_error; // Inherit constructors.
  };

}; // class MDOffloadDriver

class MDOffloadUser : public FilterUser {
public:
  MDOffloadUser(std::unique_ptr<User> next)
      : FilterUser(std::move(next))
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
  MDOffloadBucket(std::unique_ptr<Bucket> next, User* user)
      : FilterBucket(std::move(next), user)
  {
  }
  virtual ~MDOffloadBucket() = default;

  virtual Attrs& get_attrs() override;
  virtual int set_attrs(Attrs a) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) override;

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;

}; // class MDOffloadFilterBucket

class MDOffloadObject : public FilterObject {

  /**
   * @brief Cached object attributes.
   *
   * We're not as constrained here as with MDOffloadBucket::cached_attrs_
   * because the API doesn't require us to return a reference to this field.
   * However, we keep it simple for consistency.
   */
  rgw::sal::Attrs cached_attrs_;

public:
  MDOffloadObject(std::unique_ptr<Object> next, Bucket* bucket)
      : FilterObject(std::move(next), bucket)
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
