/**
 * @file rgw_ubns_machine.h
 * @author André Lucas (alucas@akamai.com)
 * @brief UBNS state machines templates.
 * @version 0.1
 * @date 2024-04-15
 *
 * @copyright Copyright (c) 2024
 *
 */

#include "rgw_ubns.h"

namespace rgw {

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

  UBNSCreateStateMachine(const DoutPrefixProvider* dpp, std::shared_ptr<T> client, const std::string& bucket_name, const std::string cluster_id, const std::string& owner)
      : dpp_ { dpp }
      , client_ { client }
      , bucket_name_ { bucket_name }
      , cluster_id_ { cluster_id }
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
    bool nonuser_state = false; // Set to true if this is a state the user should never set.

    // Implement our state machine. Return from here if the state was handled.
    // A 'break' here means an illegal state transition was attempted, and the
    // following code will log an error.
    switch (new_state) {
    case CreateMachineState::INIT:
      nonuser_state = true;
      break;

    case CreateMachineState::CREATE_START:
      if (state_ != CreateMachineState::INIT) {
        break;
      }
      result = client_->add_bucket_entry(dpp_, bucket_name_, cluster_id_, owner_);
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
      nonuser_state = true;
      break;

    case CreateMachineState::CREATE_RPC_FAILED:
      nonuser_state = true;
      break;

    case CreateMachineState::UPDATE_START:
      if (state_ != CreateMachineState::CREATE_RPC_SUCCEEDED && state_ != CreateMachineState::UPDATE_RPC_FAILED) {
        break;
      }
      result = client_->update_bucket_entry(dpp_, bucket_name_, cluster_id_, UBNSBucketUpdateState::Created);
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
      nonuser_state = true;
      break;

    case CreateMachineState::UPDATE_RPC_FAILED:
      nonuser_state = true;
      break;

    case CreateMachineState::ROLLBACK_CREATE_START:
      if (state_ != CreateMachineState::CREATE_RPC_SUCCEEDED && state_ != CreateMachineState::UPDATE_RPC_FAILED) {
        break;
      }
      result = client_->delete_bucket_entry(dpp_, bucket_name_, cluster_id_);
      if (result.ok()) {
        state_ = CreateMachineState::ROLLBACK_CREATE_SUCCEEDED;
        return true;
      } else {
        state_ = CreateMachineState::ROLLBACK_CREATE_FAILED;
        saved_result_ = result;
        return false;
      }
      break;

    case CreateMachineState::ROLLBACK_CREATE_SUCCEEDED:
      nonuser_state = true;
      break;

    case CreateMachineState::ROLLBACK_CREATE_FAILED:
      nonuser_state = true;
      break;

    case CreateMachineState::COMPLETE:
      if (state_ != CreateMachineState::UPDATE_RPC_SUCCEEDED) {
        break;
      }
      state_ = CreateMachineState::COMPLETE;
      return true;
    }
    // If we didn't return directly from the state switch, we're attempting an invalid
    // transition.
    ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNSCreateMachine: invalid state transition {} -> {}"), to_str(state_), to_str(new_state)) << dendl;
    if (nonuser_state) {
      ceph_assertf_always(false, "%s: invalid user state transition %s -> %a attempted", __func__, to_str(state_).c_str(), to_str(new_state).c_str());
    }
    return false;
  }

  std::optional<UBNSClientResult> saved_grpc_result() const noexcept { return saved_result_; }

private:
  const DoutPrefixProvider* dpp_;
  std::shared_ptr<T> client_;
  std::string bucket_name_;
  std::string cluster_id_;
  std::string owner_;
  CreateMachineState state_;
  std::optional<UBNSClientResult> saved_result_;
}; // class UBNSCreateMachine

/// Convenience typedef for rgw_op.cc.
using UBNSCreateMachine = UBNSCreateStateMachine<UBNSClient>;
/// Convenience typedef for rgw_op.cc.
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

  UBNSDeleteStateMachine(const DoutPrefixProvider* dpp, std::shared_ptr<T> client, const std::string& bucket_name, const std::string& cluster_id, const std::string& owner)
      : dpp_ { dpp }
      , client_ { client }
      , bucket_name_ { bucket_name }
      , cluster_id_ { cluster_id }
      , owner_ { owner }
      , state_ { DeleteMachineState::INIT }
  {
  }

  ~UBNSDeleteStateMachine()
  {
    ldpp_dout(dpp_, 20) << fmt::format(FMT_STRING("~UBNSDeleteMachine")) << dendl;
    if (state_ == DeleteMachineState::UPDATE_RPC_SUCCEEDED) {
      ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: rolling back bucket deletion update for {}"), bucket_name_) << dendl;
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
    bool nonuser_state = false;

    // Implement our state machine. A 'break' means an illegal state
    // transition.
    switch (new_state) {
    case DeleteMachineState::INIT:
      nonuser_state = true;
      break;

    case DeleteMachineState::UPDATE_START:
      if (state_ != DeleteMachineState::INIT) {
        break;
      }
      result = client_->update_bucket_entry(dpp_, bucket_name_, cluster_id_, UBNSBucketUpdateState::Deleting);
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
      nonuser_state = true;
      break;

    case DeleteMachineState::UPDATE_RPC_FAILED:
      nonuser_state = true;
      break;

    case DeleteMachineState::DELETE_START:
      if (state_ != DeleteMachineState::UPDATE_RPC_SUCCEEDED && state_ != DeleteMachineState::DELETE_RPC_FAILED) {
        break;
      }
      result = client_->delete_bucket_entry(dpp_, bucket_name_, cluster_id_);
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
      nonuser_state = true;
      break;

    case DeleteMachineState::DELETE_RPC_FAILED:
      nonuser_state = true;
      break;

    case DeleteMachineState::ROLLBACK_UPDATE_START:
      if (state_ != DeleteMachineState::UPDATE_RPC_SUCCEEDED && state_ != DeleteMachineState::DELETE_RPC_FAILED) {
        break;
      }
      ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNS: rolling back bucket deletion update for {} / {}"), bucket_name_, cluster_id_) << dendl;
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

    case DeleteMachineState::ROLLBACK_UPDATE_SUCCEEDED:
      nonuser_state = true;
      break;

    case DeleteMachineState::ROLLBACK_UPDATE_FAILED:
      nonuser_state = true;
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
    ldpp_dout(dpp_, 1) << fmt::format(FMT_STRING("UBNSDeleteMachine: invalid state transition {} -> {}"), to_str(state_), to_str(new_state)) << dendl;
    if (nonuser_state) {
      ceph_assertf_always(false, "%s: invalid user state transition %s -> %s attempted", __func__, to_str(state_).c_str(), to_str(new_state).c_str());
    }
    return false;
  }

  std::optional<UBNSClientResult> saved_grpc_result() const noexcept { return saved_result_; }

private:
  const DoutPrefixProvider* dpp_;
  std::shared_ptr<T> client_;
  std::string bucket_name_;
  std::string cluster_id_;
  std::string owner_;
  DeleteMachineState state_;
  std::optional<UBNSClientResult> saved_result_;
}; // class UBNSDeleteMachine

/// Convenience typedef for rgw_op.cc.
using UBNSDeleteMachine = UBNSDeleteStateMachine<UBNSClient>;
/// Convenience typedef for rgw_op.cc.
using UBNSDeleteState = UBNSDeleteMachine::DeleteMachineState;

} // namespace rgw
