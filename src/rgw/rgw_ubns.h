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
  UBNSClientResult update_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& owner);
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
  enum class State {
    Init,
    CreateStart,
    CreateRPCSucceeded,
    CreateRPCFailed,
    UpdateStart,
    UpdateRPCSucceeded,
    UpdateRPCFailed,
    RollbackCreateStart,
    RollbackCreateFailed,
    Complete,
  }; // enum class UBNSCreateMachine::State

  std::string to_str(State state)
  {
    switch (state) {
    case State::Init:
      return "Init";
    case State::CreateStart:
      return "CreateStart";
    case State::CreateRPCSucceeded:
      return "CreateRPCSucceeded";
    case State::CreateRPCFailed:
      return "CreateFailed";
    case State::UpdateStart:
      return "UpdateStart";
    case State::UpdateRPCSucceeded:
      return "UpdateRPCSucceeded";
    case State::UpdateRPCFailed:
      return "UpdateRPCFailed";
    case State::RollbackCreateStart:
      return "RollbackCreateStart";
    case State::RollbackCreateFailed:
      return "RollbackCreateFailed";
    case State::Complete:
      return "Complete";
    }
  } // UBNSCreateMachine::to_str(State)

  UBNSCreateStateMachine(const DoutPrefixProvider* dpp, std::shared_ptr<UBNSClient> client, const std::string& bucket_name, const std::string& owner)
      : dpp_ { dpp }
      , client_ { client }
      , bucket_name_ { bucket_name }
      , owner_ { owner }
      , state_ { State::Init }
  {
  }

  ~UBNSCreateStateMachine()
  {
    ldpp_dout(dpp_, 20) << fmt::format(FMT_STRING("~UBNSCreateMachine")) << dendl;
    if (state_ == State::CreateRPCSucceeded) {
      ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: Rolling back bucket creation for {}"), bucket_name_) << dendl;
      // Start the rollback. Ignore the result.
      (void)set_state(State::RollbackCreateStart);
    }
    ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("~UBNSCreateMachine bucket '{}' owner '{}' end state {}"), bucket_name_, owner_, to_str(state_)) << dendl;
  }

  State state() const noexcept { return state_; }
  bool set_state(State new_state) noexcept
  {
    ldpp_dout(dpp_, 20) << fmt::format(FMT_STRING("UBNSCreateMachine: attempt state transition {} -> {}"), to_str(state_), to_str(new_state)) << dendl;
    UBNSClientResult result;

    // Implement our state machine. A 'break' means an illegal state
    // transition.
    switch (new_state) {
    case State::Init:
      break;

    case State::CreateStart:
      if (state_ != State::Init) {
        break;
      }
      result = client_->add_bucket_entry(dpp_, bucket_name_, owner_);
      if (result.ok()) {
        state_ = State::CreateRPCSucceeded;
        return true;
      } else {
        ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: add_bucket_entry() rpc for bucket {} failed: {}"), bucket_name_, result.to_string()) << dendl;
        state_ = State::CreateRPCFailed;
        saved_result_ = result;
        return false;
      }
      break;

    case State::CreateRPCSucceeded:
    case State::CreateRPCFailed:
      break;

    case State::UpdateStart:
      if (state_ != State::CreateRPCSucceeded && state_ != State::UpdateRPCFailed) {
        break;
      }
      result = client_->update_bucket_entry(dpp_, bucket_name_, owner_);
      if (result.ok()) {
        state_ = State::UpdateRPCSucceeded;
        return true;
      } else {
        ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: update_bucket_entry() rpc for bucket {} failed: {}"), bucket_name_, result.to_string()) << dendl;
        state_ = State::UpdateRPCFailed;
        saved_result_ = result;
        return false;
      }
      break;

    case State::UpdateRPCSucceeded:
      break;

    case State::UpdateRPCFailed:
      break;

    case State::RollbackCreateStart:
      if (state_ != State::CreateRPCSucceeded && state_ != State::UpdateRPCFailed) {
        break;
      }
      result = client_->delete_bucket_entry(dpp_, bucket_name_);
      if (result.ok()) {
        state_ = State::Complete;
        return true;
      } else {
        state_ = State::RollbackCreateFailed;
        saved_result_ = result;
        return false;
      }
      break;

    case State::RollbackCreateFailed:
      break;

    case State::Complete:
      break;
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
  State state_;
  std::optional<UBNSClientResult> saved_result_;
}; // class UBNSCreateMachine

using UBNSCreateMachine = UBNSCreateStateMachine<UBNSClient>;

template <typename T>
class UBNSDeleteStateMachine {

public:
  enum class State {
    Init,
    UpdateStart,
    UpdateRPCSucceeded,
    UpdateRPCFailed,
    DeleteStart,
    DeleteRPCSucceeded,
    DeleteRPCFailed,
    RollbackUpdateStart,
    RollbackUpdateFailed,
    Complete
  }; // enum class UBNSDeleteMachine::State

  std::string to_str(State state)
  {
    switch (state) {
    case State::Init:
      return "Init";
    case State::UpdateStart:
      return "UpdateStart";
    case State::UpdateRPCSucceeded:
      return "UpdateRPCSucceeded";
    case State::UpdateRPCFailed:
      return "UpdateRPCFailed";
    case State::DeleteStart:
      return "DeleteStart";
    case State::DeleteRPCSucceeded:
      return "DeleteRPCSucceeded";
    case State::DeleteRPCFailed:
      return "DeleteRPCFailed";
    case State::RollbackUpdateStart:
      return "RollbackUpdateStart";
    case State::RollbackUpdateFailed:
      return "RollbackUpdateFailed";
    case State::Complete:
      return "Complete";
    }
  }; // UBNSDeleteMachine::to_str(State)

  UBNSDeleteStateMachine(const DoutPrefixProvider* dpp, std::shared_ptr<UBNSClient> client, const std::string& bucket_name, const std::string& owner)
      : dpp_ { dpp }
      , client_ { client }
      , bucket_name_ { bucket_name }
      , owner_ { owner }
      , state_ { State::Init }
  {
  }

  ~UBNSDeleteStateMachine()
  {
    ldpp_dout(dpp_, 20) << fmt::format(FMT_STRING("~UBNSDeleteMachine")) << dendl;
    if (state_ == State::UpdateRPCSucceeded) {
      ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: Rolling back bucket deletion update for {}"), bucket_name_) << dendl;
      // Start the rollback. Ignore the result.
      (void)set_state(State::RollbackUpdateStart);
    }
    ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("~UBNSDeleteMachine bucket '{}' owner '{}' end state {}"), bucket_name_, owner_, to_str(state_)) << dendl;
  }

  State state() const noexcept { return state_; }
  bool set_state(State new_state) noexcept
  {
    ldpp_dout(dpp_, 20) << fmt::format(FMT_STRING("UBNSDeleteMachine: attempt state transition {} -> {}"), to_str(state_), to_str(state_)) << dendl;
    UBNSClientResult result;

    // Implement our state machine. A 'break' means an illegal state
    // transition.
    switch (new_state) {
    case State::Init:
      break;

    case State::UpdateStart:
      if (state_ != State::Init) {
        break;
      }
      result = client_->update_bucket_entry(dpp_, bucket_name_, owner_);
      if (result.ok()) {
        state_ = State::UpdateRPCSucceeded;
        return true;
      } else {
        ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: update_bucket_entry() rpc for bucket {} failed: {}"), bucket_name_, result.to_string()) << dendl;
        state_ = State::UpdateRPCFailed;
        saved_result_ = result;
        return false;
      }
      break;

    case State::UpdateRPCSucceeded:
      break;

    case State::UpdateRPCFailed:
      break;

    case State::DeleteStart:
      if (state_ != State::UpdateRPCSucceeded && state_ != State::DeleteRPCFailed) {
        break;
      }
      result = client_->delete_bucket_entry(dpp_, bucket_name_);
      if (result.ok()) {
        state_ = State::DeleteRPCSucceeded;
        return true;
      } else {
        ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: delete_bucket_entry() rpc for bucket {} failed: {}"), bucket_name_, result.to_string()) << dendl;
        state_ = State::DeleteRPCFailed;
        saved_result_ = result;
        return false;
      }
      break;

    case State::DeleteRPCSucceeded:
    case State::DeleteRPCFailed:
      break;

    case State::RollbackUpdateStart:
      if (state_ != State::UpdateRPCSucceeded && state_ != State::DeleteRPCFailed) {
        break;
      }
      result = client_->update_bucket_entry(dpp_, bucket_name_, owner_);
      if (result.ok()) {
        state_ = State::Complete;
        return true;
      } else {
        state_ = State::RollbackUpdateFailed;
        saved_result_ = result;
        return false;
      }
      break;

    case State::RollbackUpdateFailed:
      break;

    case State::Complete:
      break;
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
  State state_;
  std::optional<UBNSClientResult> saved_result_;
}; // class UBNSDeleteMachine

using UBNSDeleteMachine = UBNSDeleteStateMachine<UBNSClient>;

} // namespace rgw
