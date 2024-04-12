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

#include <fmt/format.h>

#include "common/dout.h"

namespace rgw {

class UBNSClientImpl; // Forward declaration.

class UBNSClientResult {
private:
  int code_;
  std::string message_;
  bool success_;

private:
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
  explicit UBNSClientResult(int code, const std::string& message)
      : code_ { code }
      , message_ { message }
      , success_ { false }
  {
  }

public:
  /**
   * @brief Construct an empty Result object.
   *
   * Use only if you have to, e.g. as a default constructor outside a switch
   * block.
   *
   * For clarity, prefer to use the static success() method to create an
   * object indicating success, or a failure() method to create an object
   * indicating failure.
   *
   */
  UBNSClientResult()
      : code_ { 0 }
      , message_ {}
      , success_ { true }
  {
  }

  // Copies and moves are fine.
  UBNSClientResult(const UBNSClientResult&) = default;
  UBNSClientResult& operator=(const UBNSClientResult&) = default;
  UBNSClientResult(UBNSClientResult&&) = default;
  UBNSClientResult& operator=(UBNSClientResult&&) = default;

  /**
   * @brief Return a success-type result.
   *
   * @return Result A result indicating success.
   */
  static UBNSClientResult success()
  {
    return UBNSClientResult();
  }

  /**
   * @brief Return an error-type result.
   *
   * @param code The error code.
   * @param message The human-readable error message.
   * @return Result A result object constructed with the given fields.
   */
  static UBNSClientResult error(int code, const std::string& message)
  {
    return UBNSClientResult(code, message);
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
  friend std::ostream& operator<<(std::ostream& os, const UBNSClientResult& ep);
};

enum class UBNSBucketUpdateState {
  Unspecified,
  Created,
  Deleting
}; // enum class UBNSBucketUpdateState

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

  /**
   * @brief Pass to the implementation's add_bucket_entry().
   *
   * @param dpp DoutPrefixProvider.
   * @param bucket_name The bucket name.
   * @return UBNSClient::Result the implementation's result object.
   */
  UBNSClientResult add_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& owner);

  /**
   * @brief Pass to the implementation's delete_bucket_entry().
   *
   * @param dpp DoutPrefixProvider.
   * @param bucket_name The bucket name.
   * @return UBNSClient::Result the implementation's result object.
   */
  UBNSClientResult delete_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name);

  /**
   * @brief Pass to the implementation's update_bucket_entry().
   *
   * @param dpp DoutPrefixProvider.
   * @param bucket_name The bucket name.
   * @return UBNSClient::Result the implementation's result object.
   */
  UBNSClientResult update_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& owner, UBNSBucketUpdateState state);
};

/**
 * @brief State machine implementing the client Create side of the UBNS
 * protocol.
 *
 * XXX
 */
template <typename T>
class UBNSCreateStateMachine {

public:
  enum class CreateMachineState {
    INIT,
    CREATE_START,
    CREATE_RPC_SUCCEEDED,
    CREATE_RPC_FAILED,
    UPDATE_START,
    UPDATE_RPC_SUCCEEDED,
    UPDATE_RPC_FAILED,
    ROLLBACK_CREATE_START,
    ROLLBACK_CREATE_SUCCEEDED,
    ROLLBACK_CREATE_FAILED,
    COMPLETE,
  }; // enum class UBNSCreateMachine::State

  std::string to_str(CreateMachineState state)
  {
    switch (state) {
    case CreateMachineState::INIT:
      return "INIT";
    case CreateMachineState::CREATE_START:
      return "CREATE_START";
    case CreateMachineState::CREATE_RPC_SUCCEEDED:
      return "CREATE_RPC_SUCCEEDED";
    case CreateMachineState::CREATE_RPC_FAILED:
      return "CREATE_FAILED";
    case CreateMachineState::UPDATE_START:
      return "UPDATE_START";
    case CreateMachineState::UPDATE_RPC_SUCCEEDED:
      return "UPDATE_RPC_SUCCEEDED";
    case CreateMachineState::UPDATE_RPC_FAILED:
      return "UPDATE_RPC_FAILED";
    case CreateMachineState::ROLLBACK_CREATE_START:
      return "ROLLBACK_CREATE_START";
    case CreateMachineState::ROLLBACK_CREATE_SUCCEEDED:
      return "ROLLBACK_CREATE_SUCCEEDED";
    case CreateMachineState::ROLLBACK_CREATE_FAILED:
      return "ROLLBACK_CREATE_FAILED";
    case CreateMachineState::COMPLETE:
      return "COMPLETE";
    }
  } // UBNSCreateMachine::to_str(State)

  UBNSCreateStateMachine(const DoutPrefixProvider* dpp, std::shared_ptr<T> client, const std::string& bucket_name, const std::string& owner)
      : dpp_ { dpp }
      , client_ { client }
      , bucket_name_ { bucket_name }
      , owner_ { owner }
      , state_ { CreateMachineState::INIT }
  {
  }

  ~UBNSCreateStateMachine()
  {
    ldpp_dout(dpp_, 20) << fmt::format(FMT_STRING("~UBNSCreateMachine")) << dendl;
    if (state_ == CreateMachineState::CREATE_RPC_SUCCEEDED) {
      ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: Rolling back bucket creation for {}"), bucket_name_) << dendl;
      // Start the rollback. Ignore the result.
      (void)set_state(CreateMachineState::ROLLBACK_CREATE_START);
    }
    ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("~UBNSCreateMachine bucket '{}' owner '{}' end state {}"), bucket_name_, owner_, to_str(state_)) << dendl;
  }

  CreateMachineState state() const noexcept { return state_; }

  bool set_state(CreateMachineState new_state) noexcept
  {
    ldpp_dout(dpp_, 20) << fmt::format(FMT_STRING("UBNSCreateMachine: attempt state transition {} -> {}"), to_str(state_), to_str(new_state)) << dendl;
    UBNSClientResult result;

    // Implement our state machine. A 'break' means an illegal state
    // transition.
    switch (new_state) {
    case CreateMachineState::INIT:
      break;

    case CreateMachineState::CREATE_START:
      if (state_ != CreateMachineState::INIT) {
        break;
      }
      result = client_->add_bucket_entry(dpp_, bucket_name_, owner_);
      if (result.ok()) {
        state_ = CreateMachineState::CREATE_RPC_SUCCEEDED;
        return true;
      } else {
        ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: add_bucket_entry() rpc for bucket {} failed: {}"), bucket_name_, result.to_string()) << dendl;
        state_ = CreateMachineState::CREATE_RPC_FAILED;
        saved_result_ = result;
        return false;
      }
      break;

    case CreateMachineState::CREATE_RPC_SUCCEEDED:
    case CreateMachineState::CREATE_RPC_FAILED:
      break;

    case CreateMachineState::UPDATE_START:
      if (state_ != CreateMachineState::CREATE_RPC_SUCCEEDED && state_ != CreateMachineState::UPDATE_RPC_FAILED) {
        break;
      }
      result = client_->update_bucket_entry(dpp_, bucket_name_, owner_, UBNSBucketUpdateState::Created);
      if (result.ok()) {
        state_ = CreateMachineState::UPDATE_RPC_SUCCEEDED;
        return true;
      } else {
        ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: update_bucket_entry() rpc for bucket {} failed: {}"), bucket_name_, result.to_string()) << dendl;
        state_ = CreateMachineState::UPDATE_RPC_FAILED;
        saved_result_ = result;
        return false;
      }
      break;

    case CreateMachineState::UPDATE_RPC_SUCCEEDED:
      break;

    case CreateMachineState::UPDATE_RPC_FAILED:
      break;

    case CreateMachineState::ROLLBACK_CREATE_START:
      if (state_ != CreateMachineState::CREATE_RPC_SUCCEEDED && state_ != CreateMachineState::UPDATE_RPC_FAILED) {
        break;
      }
      result = client_->delete_bucket_entry(dpp_, bucket_name_);
      if (result.ok()) {
        state_ = CreateMachineState::ROLLBACK_CREATE_SUCCEEDED;
        return true;
      } else {
        state_ = CreateMachineState::ROLLBACK_CREATE_FAILED;
        saved_result_ = result;
        return false;
      }
      break;

    case CreateMachineState::ROLLBACK_CREATE_FAILED:
      break;

    case CreateMachineState::COMPLETE:
      if (state_ != CreateMachineState::UPDATE_RPC_SUCCEEDED) {
        break;
      }
      state_ = CreateMachineState::COMPLETE;
      return true;
    }
    // If we didn't return from the state switch, we're attempting an invalid
    // transition.
    ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNSCreateMachine: Invalid state transition {} -> {}"), to_str(state_), to_str(new_state)) << dendl;
    return false;
  }

  std::optional<UBNSClientResult> saved_grpc_result() const noexcept { return saved_result_; }

private:
  const DoutPrefixProvider* dpp_;
  std::shared_ptr<T> client_;
  std::string bucket_name_;
  std::string owner_;
  CreateMachineState state_;
  std::optional<UBNSClientResult> saved_result_;
}; // class UBNSCreateMachine

using UBNSCreateMachine = UBNSCreateStateMachine<UBNSClient>;
using UBNSCreateState = UBNSCreateMachine::CreateMachineState;

template <typename T>
class UBNSDeleteStateMachine {

public:
  enum class DeleteMachineState {
    INIT,
    UPDATE_START,
    UPDATE_RPC_SUCCEEDED,
    UPDATE_RPC_FAILED,
    DELETE_START,
    DELETE_RPC_SUCCEEDED,
    DELETE_RPC_FAILED,
    ROLLBACK_UPDATE_START,
    ROLLBACK_UPDATE_SUCCEEDED,
    ROLLBACK_UPDATE_FAILED,
    COMPLETE
  }; // enum class UBNSDeleteMachine::State

  std::string to_str(DeleteMachineState state)
  {
    switch (state) {
    case DeleteMachineState::INIT:
      return "INIT";
    case DeleteMachineState::UPDATE_START:
      return "UPDATE_START";
    case DeleteMachineState::UPDATE_RPC_SUCCEEDED:
      return "UPDATE_RPC_SUCCEEDED";
    case DeleteMachineState::UPDATE_RPC_FAILED:
      return "UPDATE_RPC_FAILED";
    case DeleteMachineState::DELETE_START:
      return "DELETE_START";
    case DeleteMachineState::DELETE_RPC_SUCCEEDED:
      return "DELETE_RPC_SUCCEEDED";
    case DeleteMachineState::DELETE_RPC_FAILED:
      return "DELETE_RPC_FAILED";
    case DeleteMachineState::ROLLBACK_UPDATE_START:
      return "ROLLBACK_UPDATE_START";
    case DeleteMachineState::ROLLBACK_UPDATE_SUCCEEDED:
      return "ROLLBACK_UPDATE_SUCCEEDED";
    case DeleteMachineState::ROLLBACK_UPDATE_FAILED:
      return "ROLLBACK_UPDATE_FAILED";
    case DeleteMachineState::COMPLETE:
      return "COMPLETE";
    }
  }; // UBNSDeleteMachine::to_str(State)

  UBNSDeleteStateMachine(const DoutPrefixProvider* dpp, std::shared_ptr<T> client, const std::string& bucket_name, const std::string& owner)
      : dpp_ { dpp }
      , client_ { client }
      , bucket_name_ { bucket_name }
      , owner_ { owner }
      , state_ { DeleteMachineState::INIT }
  {
  }

  ~UBNSDeleteStateMachine()
  {
    ldpp_dout(dpp_, 20) << fmt::format(FMT_STRING("~UBNSDeleteMachine")) << dendl;
    if (state_ == DeleteMachineState::UPDATE_RPC_SUCCEEDED) {
      ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: Rolling back bucket deletion update for {}"), bucket_name_) << dendl;
      // Start the rollback. Ignore the result.
      (void)set_state(DeleteMachineState::ROLLBACK_UPDATE_START);
    }
    ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("~UBNSDeleteMachine bucket '{}' owner '{}' end state {}"), bucket_name_, owner_, to_str(state_)) << dendl;
  }

  DeleteMachineState state() const noexcept { return state_; }

  bool set_state(DeleteMachineState new_state) noexcept
  {
    ldpp_dout(dpp_, 20) << fmt::format(FMT_STRING("UBNSDeleteMachine: attempt state transition {} -> {}"), to_str(state_), to_str(new_state)) << dendl;
    UBNSClientResult result;

    // Implement our state machine. A 'break' means an illegal state
    // transition.
    switch (new_state) {
    case DeleteMachineState::INIT:
      break;

    case DeleteMachineState::UPDATE_START:
      if (state_ != DeleteMachineState::INIT) {
        break;
      }
      result = client_->update_bucket_entry(dpp_, bucket_name_, owner_, UBNSBucketUpdateState::Deleting);
      if (result.ok()) {
        state_ = DeleteMachineState::UPDATE_RPC_SUCCEEDED;
        return true;
      } else {
        ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: update_bucket_entry() rpc for bucket {} failed: {}"), bucket_name_, result.to_string()) << dendl;
        state_ = DeleteMachineState::UPDATE_RPC_FAILED;
        saved_result_ = result;
        return false;
      }
      break;

    case DeleteMachineState::UPDATE_RPC_SUCCEEDED:
      break;

    case DeleteMachineState::UPDATE_RPC_FAILED:
      break;

    case DeleteMachineState::DELETE_START:
      if (state_ != DeleteMachineState::UPDATE_RPC_SUCCEEDED && state_ != DeleteMachineState::DELETE_RPC_FAILED) {
        break;
      }
      result = client_->delete_bucket_entry(dpp_, bucket_name_);
      if (result.ok()) {
        state_ = DeleteMachineState::DELETE_RPC_SUCCEEDED;
        return true;
      } else {
        ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: delete_bucket_entry() rpc for bucket {} failed: {}"), bucket_name_, result.to_string()) << dendl;
        state_ = DeleteMachineState::DELETE_RPC_FAILED;
        saved_result_ = result;
        return false;
      }
      break;

    case DeleteMachineState::DELETE_RPC_SUCCEEDED:
      break;

    case DeleteMachineState::DELETE_RPC_FAILED:
      break;

    case DeleteMachineState::ROLLBACK_UPDATE_START:
      if (state_ != DeleteMachineState::UPDATE_RPC_SUCCEEDED && state_ != DeleteMachineState::DELETE_RPC_FAILED) {
        break;
      }
      result = client_->update_bucket_entry(dpp_, bucket_name_, owner_, UBNSBucketUpdateState::Created);
      if (result.ok()) {
        state_ = DeleteMachineState::ROLLBACK_UPDATE_SUCCEEDED;
        return true;
      } else {
        state_ = DeleteMachineState::ROLLBACK_UPDATE_FAILED;
        saved_result_ = result;
        return false;
      }
      break;

    case DeleteMachineState::ROLLBACK_UPDATE_FAILED:
      break;

    case DeleteMachineState::COMPLETE:
      if (state_ != DeleteMachineState::DELETE_RPC_SUCCEEDED) {
        break;
      }
      state_ = DeleteMachineState::COMPLETE;
      return true;
    }

    // If we didn't return from the state switch, we're attempting an invalid
    // transition.
    ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNSDeleteMachine: Invalid state transition {} -> {}"), to_str(state_), to_str(new_state)) << dendl;
    return false;
  }

  std::optional<UBNSClientResult> saved_grpc_result() const noexcept { return saved_result_; }

private:
  const DoutPrefixProvider* dpp_;
  std::shared_ptr<T> client_;
  std::string bucket_name_;
  std::string owner_;
  DeleteMachineState state_;
  std::optional<UBNSClientResult> saved_result_;
}; // class UBNSDeleteMachine

using UBNSDeleteMachine = UBNSDeleteStateMachine<UBNSClient>;
using UBNSDeleteState = UBNSDeleteMachine::DeleteMachineState;

} // namespace rgw
