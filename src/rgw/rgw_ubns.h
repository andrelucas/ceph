/**
 * @file rgw_ubns.h
 * @author André Lucas (alucas@akamai.com)
 * @brief Unique Bucket Naming System (UBNS) implementation.
 * @version 0.1
 * @date 2024-03-20
 *
 * @copyright Copyright (c) 2024
 *
 */

#pragma once

#include <memory>

namespace rgw {

class UBNSClientImpl; // Forward declaration.

class UBNSClient {
private:
  std::unique_ptr<UBNSClientImpl> impl_;

public:
  UBNSClient();
  ~UBNSClient();

  UBNSClient(const UBNSClient&) = delete;
  UBNSClient& operator=(const UBNSClient&) = delete;
  UBNSClient(UBNSClient&&) = delete;
  UBNSClient& operator=(UBNSClient&&) = delete;

  /**
   * @brief Initialise the UBNS client. This will create a gRPC channel, so we
   * need access to configuration.
   *
   * @return true Success, UBNS may be used.
   * @return false Failure, the probably best action is to fail startup.
   */
  bool init();

  /**
   * @brief Shut down the UBNS client and free resources.
   */
  void shutdown();
};

} // namespace rgw
