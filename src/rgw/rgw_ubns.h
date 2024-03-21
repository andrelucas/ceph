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

#include <iosfwd>
#include <memory>
#include <string>

#include "common/dout.h"

namespace rgw {

class UBNSClientImpl; // Forward declaration.

/**
 * @brief UBNS client class. A shell for UBNSClientImpl.
 *
 * A pointer to an object of this class, created at startup, is passed around
 * inside RGW. This class passes all useful methods through to its internal
 * UBNSClientImpl object, created at construction, that performs all useful
 * work.
 *
 * This allows us to keep the gRPC headers away from most of RGW. It helps
 * compilation time, and hopes to limit future compatibility problems by
 * minimising the codebase's exposure to gRPC, a large and complex codebase
 * with its own dependencies and quirks.
 */
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
  bool init(CephContext* cct, const std::string& grpc_uri);

  /**
   * @brief Shut down the UBNS client and free resources.
   */
  void shutdown();

  /**
   * @brief Return type for RPC operations.
   */
  class Result {
  private:
    bool success_;
    std::string message_;

  public:
    /// Construct a success-type result.
    Result()
        : success_(true)
    {
    }
    // Constuct a failure-type result with an error message.
    explicit Result(const std::string& message)
        : success_(false)
        , message_(message)
    {
    }

    bool ok() const { return success_; }
    bool err() const { return !ok(); }
    const std::string& message() const { return message_; }

    std::string to_string() const;
    friend std::ostream& operator<<(std::ostream& os, const UBNSClient::Result& ep);
  };

  /**
   * @brief Pass to the implementation's add_bucket_entry().
   *
   * @param dpp DoutPrefixProvider.
   * @param bucket_name The bucket name.
   * @return UBNSClient::Result the implementation's result object.
   */
  Result add_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name);

  /**
   * @brief Pass to the implementation's delete_bucket_entry().
   *
   * @param dpp DoutPrefixProvider.
   * @param bucket_name The bucket name.
   * @return UBNSClient::Result the implementation's result object.
   */
  Result delete_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name);

  /**
   * @brief Pass to the implementation's update_bucket_entry().
   *
   * @param dpp DoutPrefixProvider.
   * @param bucket_name The bucket name.
   * @return UBNSClient::Result the implementation's result object.
   */
  Result update_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name);
};

} // namespace rgw
