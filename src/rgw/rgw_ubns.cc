/**
 * @file rgw_ubns.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Unique Bucket Naming System (UBNS) implementation.
 * @version 0.1
 * @date 2024-03-20
 *
 * @copyright Copyright (c) 2024
 *
 */

#include "rgw_ubns.h"
#include "rgw_ubns_impl.h"

#include <fmt/format.h>
#include <iostream>
#include <memory>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace rgw {

// This has to be here, in a .cc file where we know the size of
// UBNSClientImpl. It can't be in the header file. See
// https://www.fluentcpp.com/2017/09/22/make-pimpl-using-unique_ptr/ .
UBNSClient::UBNSClient()
    : impl_(std::make_unique<UBNSClientImpl>())
{
}

// This has to be here, in a .cc file where we know the size of
// UBNSClientImpl. It can't be in the header file. See
// https://www.fluentcpp.com/2017/09/22/make-pimpl-using-unique_ptr/ .
UBNSClient::~UBNSClient() { }

bool UBNSClient::init(CephContext* cct, const std::string& grpc_uri)
{
  return impl_->init(cct, grpc_uri);
}

void UBNSClient::shutdown()
{
  impl_->shutdown();
}

UBNSClientResult UBNSClient::add_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& cluster_id, const std::string& owner)
{
  return impl_->add_bucket_entry(dpp, bucket_name, cluster_id, owner);
}
UBNSClientResult UBNSClient::delete_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& cluster_id)
{
  return impl_->delete_bucket_entry(dpp, bucket_name, cluster_id);
}
UBNSClientResult UBNSClient::update_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& cluster_id, UBNSBucketUpdateState state)
{
  return impl_->update_bucket_entry(dpp, bucket_name, cluster_id, state);
}

std::string UBNSClientResult::to_string() const
{
  if (ok()) {
    return "UBNSClientResult(success,code=0)";
  } else {
    return fmt::format(FMT_STRING("UBNSClientResult(failure,code={},message='{}')"), code(), message());
  }
}

std::ostream& operator<<(std::ostream& os, const UBNSClientResult& r)
{
  os << r.to_string();
  return os;
}

} // namespace rgw
