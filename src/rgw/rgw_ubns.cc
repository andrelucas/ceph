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

#include <memory>

// #include "common/dout.h"
// #include "rgw/rgw_common.h"

#include "ubdb/v1/ubdb.grpc.pb.h"
#include "ubdb/v1/ubdb.pb.h"

// These are 'standard' protobufs for the 'Richer error model'
// (https://grpc.io/docs/guides/error/).
#include "google/rpc/error_details.pb.h"
#include "google/rpc/status.pb.h"

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

bool UBNSClient::init()
{
  return impl_->init();
}

void UBNSClient::shutdown()
{
  impl_->shutdown();
}

} // namespace rgw
