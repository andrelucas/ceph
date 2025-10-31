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
#include "rgw_common.h"
#include <cstddef>

#define dout_subsys ceph_subsys_rgw

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

/**
 * @brief Dump attributes in a way meaningful to us.
 *
 * @param attrs
 * @return std::string a string representation of the attributes.
 */
static std::string dump_attrs(const rgw::sal::Attrs& attrs)
{
  std::ostringstream oss;
  oss << "{";
  bool first = true;
  for (const auto& [key, val] : attrs) {
    if (!first) {
      oss << ", ";
    }
    first = false;
    oss << key << ": " << val;
  }
  oss << "}";
  return oss.str();
}

/****************************************************************************/

// MDOffloadFilterDriver

const std::string MDOffloadFilterDriver::get_name() const
{
  std::string name = "mdoffload<" + next->get_name() + ">";
  return name;
}

std::unique_ptr<User> MDOffloadFilterDriver::get_user(const rgw_user& u)
{
  std::unique_ptr<User> user = next->get_user(u);
  return std::make_unique<MDOffloadUser>(std::move(user));
}

int MDOffloadFilterDriver::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_access_key(dpp, key, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new MDOffloadUser(std::move(nu));
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

  User* u = new MDOffloadUser(std::move(nu));
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

  User* u = new MDOffloadUser(std::move(nu));
  user->reset(u);
  return 0;
}

int MDOffloadFilterDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  ret = next->get_bucket(dpp, nu, b, &nb, y);
  if (ret != 0)
    return ret;

  // Bucket exists. Need to preload the bucket attributes.
  // XXX

  Bucket* fb = new MDOffloadBucket(std::move(nb), u);
  bucket->reset(fb);
  return 0;
}

int MDOffloadFilterDriver::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  ret = next->get_bucket(nu, i, &nb);
  if (ret != 0)
    return ret;

  // Bucket exists. Need to preload the bucket attributes.
  // XXX

  Bucket* fb = new MDOffloadBucket(std::move(nb), u);
  bucket->reset(fb);
  return 0;
}

int MDOffloadFilterDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  // Bucket exists. Need to preload the bucket attributes.
  // XXX

  ret = next->get_bucket(dpp, nu, tenant, name, &nb, y);
  if (ret != 0)
    return ret;

  Bucket* fb = new MDOffloadBucket(std::move(nb), u);
  bucket->reset(fb);
  return 0;
}

/****************************************************************************/

// rgw::sal::MDoffloadBucket

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

  // XXX placeholder.
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadUser::create_bucket: name={} attrs={}"),
             b.name, dump_attrs(attrs))
      << dendl;

  // Pass an empty set of attributes to the next driver.
  rgw::sal::Attrs empty_attrs;
  ret = next->create_bucket(dpp, b, zonegroup_id, placement_rule, swift_ver_location, pquota_info, policy, empty_attrs, info, ep_objv, exclusive, obj_lock_enabled, existed, req_info, &nb, y);
  if (ret < 0)
    return ret;

  Bucket* fb = new MDOffloadBucket(std::move(nb), this);
  fb->set_attrs(attrs);
  bucket_out->reset(fb);

  return 0;
}

/****************************************************************************/

// rgw::sal::MDoffloadBucket

Attrs& MDOffloadBucket::get_attrs()
{
  // XXX placeholder.
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadBucket::get_attrs: attrs={}"),
             dump_attrs(cached_attrs_))
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
  // XXX placeholder.
  cached_attrs_ = a;
  ldout(g_ceph_context, 20)
      << fmt::format(FMT_STRING("MDOffloadBucket::set_attrs: attrs={}"),
             dump_attrs(cached_attrs_))
      << dendl;
  return 0;
}

int MDOffloadBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y)
{
  // XXX placeholder.
  for (auto& it : new_attrs) {
    cached_attrs_[it.first] = it.second;
  }
  ldpp_dout(dpp, 20)
      << fmt::format(FMT_STRING("MDOffloadBucket::merge_and_store_attrs: new_attrs={} cached_attrs_={}"),
             dump_attrs(new_attrs), dump_attrs(cached_attrs_))
      << dendl;
  return 0;
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
