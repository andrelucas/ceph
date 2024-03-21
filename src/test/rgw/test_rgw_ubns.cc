/**
 * @file test_rgw_ubns.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Unit tests for the Unique Bucket Name Service implementation in RGW.
 * @version 0.1
 * @date 2024-03-20
 *
 * @copyright Copyright (c) 2024
 */

#include <absl/random/random.h>

#include "rgw_ubns_impl.h"
#include "test_rgw_grpc_util.h"

#include "ubdb/v1/ubdb.grpc.pb.h"

using namespace ::ubdb::v1;
