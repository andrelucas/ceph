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

#include "rgw_sal.h"
#include "rgw_sal_filter.h"

namespace rgw::sal {

class MDOffloadDriver : public FilterDriver {
public:
  MDOffloadDriver(CephContext* cct, rgw::sal::Driver* next)
      : FilterDriver(next)
  {
  }
  virtual ~MDOffloadDriver() = default;

  /** Get a Bucket by info.  Does not query the driver, just uses the give bucket info. */
  virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) override;
  /** Lookup a Bucket by key.  Queries driver for bucket info. */
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
  /** Lookup a Bucket by name.  Queries driver for bucket info. */
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y) override;

}; // class MDOffloadDriver

class MDOffloadBucket : public FilterBucket {
public:
  MDOffloadBucket(std::unique_ptr<Bucket> next, User* user)
      : FilterBucket(std::move(next), user)
  {
  }
  virtual ~MDOffloadBucket() = default;

}; // class MDOffloadFilterBucket

} // namespace rgw::sal
