/**
 * @file rgw_handoff_grpcutil.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief gRPC support utilities.
 * @version 0.1
 * @date 2024-07-26
 *
 * @copyright Copyright (c) 2024
 *
 */

#include "rgw/rgw_handoff_grpcutil.h"

#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace rgw {

std::shared_ptr<grpc::Channel> HandoffGRPCChannel::get_channel() const
{
  std::shared_lock<chan_lock_t> lock(m_channel_);
  return channel_;
}

grpc::ChannelArguments HandoffGRPCChannel::get_default_channel_args(CephContext* const cct)
{
  grpc::ChannelArguments args;

  // Set our default backoff parameters. These are runtime-alterable.
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, cct->_conf->rgw_handoff_grpc_arg_initial_reconnect_backoff_ms);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, cct->_conf->rgw_handoff_grpc_arg_max_reconnect_backoff_ms);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, cct->_conf->rgw_handoff_grpc_arg_min_reconnect_backoff_ms);
  ldout(cct, 20) << fmt::format(FMT_STRING("HandoffGRPCChannel::{}: {}: reconnect_backoff(ms): initial/min/max={}/{}/{}"),
      __func__, description_,
      cct->_conf->rgw_handoff_grpc_arg_initial_reconnect_backoff_ms,
      cct->_conf->rgw_handoff_grpc_arg_min_reconnect_backoff_ms,
      cct->_conf->rgw_handoff_grpc_arg_max_reconnect_backoff_ms)
                 << dendl;

  return grpc::ChannelArguments();
}

void HandoffGRPCChannel::set_channel_args(CephContext* const cct, grpc::ChannelArguments& args)
{
  std::unique_lock<chan_lock_t> l { m_channel_ };
  channel_args_ = std::make_optional(args);
}

bool HandoffGRPCChannel::set_channel_uri(CephContext* const cct, const std::string& new_uri)
{
  ldout(cct, 5) << fmt::format(FMT_STRING("HandoffGRPCChannel::set_channel_uri: {}: begin set uri '{}'"), description_, new_uri) << dendl;
  std::unique_lock<chan_lock_t> g(m_channel_);
  if (!channel_args_) {
    auto args = get_default_channel_args(cct);
    // Don't use set_channel_args(), which takes lock m_channel_.
    channel_args_ = std::make_optional(std::move(args));
  }
  // XXX grpc::InsecureChannelCredentials()...
  auto new_channel = grpc::CreateCustomChannel(new_uri, grpc::InsecureChannelCredentials(), *channel_args_);
  if (!new_channel) {
    ldout(cct, 0) << fmt::format(FMT_STRING("HandoffGRPCChannel::set_channel_uri: {}: ERROR: Failed to create new gRPC channel for URI {}"), description_, new_uri) << dendl;
    return false;
  } else {
    ldout(cct, 1) << fmt::format(FMT_STRING("HandoffGRPCChannel::set_channel_uri: {}: set uri '{}' success"), description_, new_uri) << dendl;
    channel_ = std::move(new_channel);
    channel_uri_ = new_uri;
    return true;
  }
}

} // namespace rgw
