/**
 * @file rgw_ubns_impl.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Unique Bucket Naming System (UBNS) private implementation.
 * @version 0.1
 * @date 2024-03-20
 *
 * @copyright Copyright (c) 2024
 *
 */

#include "rgw_ubns_impl.h"

namespace rgw {

bool UBNSClientImpl::init()
{
  // XXX init gRPC.
  return true; // XXX
}

void UBNSClientImpl::shutdown()
{
  // XXX shut down gRPC client.
}

} // namespace rgw
