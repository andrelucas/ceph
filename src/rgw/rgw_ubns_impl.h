/**
 * @file rgw_ubns_impl.h
 * @author André Lucas (alucas@akamai.com)
 * @brief Unique Bucket Naming System (UBNS) private declarations.
 * @version 0.1
 * @date 2024-03-20
 *
 * @copyright Copyright (c) 2024
 *
 * Declarations for UBNSClientImpl and related classes.
 *
 * TRY REALLY HARD to not include this anywhere except rgw_ubns.cc and
 * rgw_ubns_impl.cc. In particular, don't add it to rgw_ubns.h no matter how
 * tempting that seems.
 *
 * This file pulls in the gRPC headers and we don't want that everywhere.
 */

#pragma once

#include <fmt/format.h>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>

#include "rgw_ubns.h"

#include "ubdb/v1/ubdb.grpc.pb.h"

namespace rgw {

/**
 * @brief Thin wrapper around the gRPC client.
 */
class UBNSgRPCClient {
private:
  std::unique_ptr<ubdb::v1::UBDBService::Stub> stub_;

public:
  /**
   * @brief Construct a new UBNSgRPCClient object given a gRPC channel.
   */
  explicit UBNSgRPCClient(std::shared_ptr<::grpc::Channel>) {};
  ~UBNSgRPCClient() {};

  // Can't copy with a unique_ptr.
  UBNSgRPCClient(const UBNSgRPCClient&) = delete;
  UBNSgRPCClient& operator=(const UBNSgRPCClient&) = delete;
  // Move is fine.
  UBNSgRPCClient(UBNSgRPCClient&&) = default;
  UBNSgRPCClient& operator=(UBNSgRPCClient&&) = default;

}; // class UBNSgRPCClient

/**
 * @brief Main implementation class for the UBNS client.
 */
class UBNSClientImpl {

public:
  UBNSClientImpl() {};
  ~UBNSClientImpl() {};

  UBNSClientImpl(const UBNSClientImpl&) = delete;
  UBNSClientImpl& operator=(const UBNSClientImpl&) = delete;
  UBNSClientImpl(UBNSClientImpl&&) = delete;
  UBNSClientImpl& operator=(UBNSClientImpl&&) = delete;

  bool init();
  void shutdown();
}; // class UBNSClientImpl

} // namespace rgw
