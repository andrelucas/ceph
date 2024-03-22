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
    int code_;
    std::string message_;
    bool success_;

  private:
    /**
     * @brief Construct a success-type Result object.
     *
     * This constructor is private. Use the static success() method to create
     * an object indicating success.
     *
     * This is the only way to generate success-type result. Since a success
     * result has no parameters, its constructor looks like a default
     * constructor and we don't want that. We want Result objects to be only
     * success or failure, with all fields defined.
     *
     */
    Result()
        : code_ { 0 }
        , message_ {}
        , success_ { true }
    {
    }

    /**
     * @brief Construct a failure-type Result object.
     *
     *  RGWOp seems to work with a mix of error code types. Some are are
     *  standard UNIX errors, e.g. EEXIST, EPERM, etc. These are defined by
     *  including <errno.h>, and are positive integers. Others are
     *  RGW-specific errors, e.g. ERR_BUCKET_EXISTS, defined in rgw_common.h.
     *
     * We're integrating with existing code. The safest thing to do is look at
     * existing code, match as close as you can the errors you return, and
     * test carefully.
     *
     * The error message is just for the logs. In the RGWOp classes we don't
     * have direct control over the error responses, we simply set a code.
     *
     * @param code The error code. Should be a positive integer of a type
     * recognised by RGW as explained above.
     * @param message A human-readable error message. Won't be seen by the
     * user but will be logged.
     */
    explicit Result(int code, const std::string& message)
        : code_ { code }
        , message_ { message }
        , success_ { false }
    {
    }

  public:
    /**
     * @brief Return a success-type result.
     *
     * @return Result A result indicating success.
     */
    static Result success()
    {
      return Result();
    }

    /**
     * @brief Return an error-type result.
     *
     * @param code The error code.
     * @param message The human-readable error message.
     * @return Result A result object constructed with the given fields.
     */
    static Result error(int code, const std::string& message)
    {
      return Result(code, message);
    }

    /**
     * @brief Return true if the operation was successful.
     *
     * @return true The operation was successful.
     * @return false The operation failed.
     */
    bool ok() const { return success_; }

    /**
     * @brief Return true if the operation failed.
     *
     * @return true The operation failed. A message and code will be
     * available for further context.
     * @return false The operation was successful.
     */
    bool err() const { return !ok(); }

    /**
     * @brief Return the error code, or zero if the operation was successful.
     *
     * @return int The error code, or zero if the operation was successful.
     */
    int code() const
    {
      if (ok()) {
        return 0;
      } else {
        return code_;
      }
    }

    /**
     * @brief Return the error message, or an empty string if the operation
     * was successful.
     *
     * @return const std::string& The error message, empty on operation
     * success.
     */
    const std::string& message() const { return message_; }

    /// Return a string representation of this object.
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
