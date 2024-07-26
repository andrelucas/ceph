/**
 * @file rgw_handoff_grpcutil.h
 * @author André Lucas (alucas@akamai.com)
 * @brief gRPC support utilities.
 * @version 0.1
 * @date 2024-07-26
 *
 * @copyright Copyright (c) 2024
 *
 */

#pragma once

#include <fmt/format.h>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>

#include <memory>
#include <optional>
#include <shared_mutex>

#include "rgw/rgw_common.h"

namespace rgw {

/**
 * @brief Thinly wrap a gRPC channel and related arguments.
 */
class HandoffGRPCChannel {

  using chan_lock_t = std::shared_mutex;

  mutable std::shared_mutex m_channel_;
  std::shared_ptr<grpc::Channel> channel_;
  std::optional<grpc::ChannelArguments> channel_args_;
  std::string channel_uri_;

public:
  /**
   * @brief Get a pointer to the channel object.
   *
   * @return std::shared_ptr<grpc::Channel>
   */
  std::shared_ptr<grpc::Channel> get_channel() const;

  /**
   * @brief Get our default grpc::ChannelArguments value.
   *
   * When calling set_channel_args(), you should first call this function to
   * get application defaults, and then modify the settings you need.
   *
   * Currently the backoff timers are set here, based on configuration
   * variables. These are runtime-alterable, but have sensible defaults.
   *
   * @return grpc::ChannelArguments A default set of channel arguments.
   */
  grpc::ChannelArguments get_default_channel_args(CephContext* const cct);

  /**
   * @brief Set the gRPC channel URI.
   *
   * This is used by init() and by the config observer. If no channel
   * arguments have been set via set_channel_args(), this will set them to the
   * default values (via get_default_channel_args).
   *
   * Do not call from auth() unless you _know_ you've not taken a lock on
   * m_config_!
   *
   * @param grpc_uri
   * @return true on success.
   * @return false on failure.
   */
  bool set_channel_uri(CephContext* const cct, const std::string& grpc_uri);

  /**
   * @brief Set custom gRPC channel arguments. Intended for testing.
   *
   * You should modify the default channel arguments obtained with
   * get_default_channel_args(). Don't start from scratch.
   *
   * Keep this simple. If you set vtable args you'll need to worry about the
   * lifetime of those is longer than the HandoffHelperImpl object that will
   * store a copy of the ChannelArguments object.
   *
   * Do not call from auth() unless you _know_ you've not taken a lock on
   * m_config_!
   *
   * @param args A populated grpc::ChannelArguments object.
   */
  void set_channel_args(CephContext* const cct, grpc::ChannelArguments& args);

}; // class HandoffGRPCChannel

} // namespace rgw
