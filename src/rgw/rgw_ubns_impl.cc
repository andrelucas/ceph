/**
 * @file rgw_ubns_impl.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Unique Bucket Naming System (UBNS) private implementation.
 * @version 0.1
 * @date 2024-03-20
 *
 * @copyright Copyright (c) 2024
 *
 */

#include "rgw_ubns_impl.h"

#include <errno.h>
#include <fstream>

#include "common/dout.h"
#include "rgw/rgw_common.h"

#include "ubdb/v1/ubdb.grpc.pb.h"
#include "ubdb/v1/ubdb.pb.h"

// These are 'standard' protobufs for the 'Richer error model'
// (https://grpc.io/docs/guides/error/).
#include "google/rpc/error_details.pb.h"
#include "google/rpc/status.pb.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace rgw {

UBNSClientResult UBNSgRPCClient::add_bucket_request(const ubdb::v1::AddBucketEntryRequest& req)
{
  ::grpc::ClientContext context;
  ubdb::v1::AddBucketEntryResponse resp;
  auto status = stub_->AddBucketEntry(&context, req, &resp);
  // XXX precise error handling.
  if (!status.ok()) {
    return UBNSClientResult::error(EEXIST, fmt::format(FMT_STRING("gRPC error: {}"), status.error_message()));
  }
  return UBNSClientResult::success();
}

UBNSClientResult UBNSgRPCClient::delete_bucket_request(const ubdb::v1::DeleteBucketEntryRequest& req)
{
  ::grpc::ClientContext context;
  ubdb::v1::DeleteBucketEntryResponse resp;
  auto status = stub_->DeleteBucketEntry(&context, req, &resp);
  // XXX precise error handling.
  if (!status.ok()) {
    return UBNSClientResult::error(ERR_NO_SUCH_BUCKET, fmt::format(FMT_STRING("gRPC error: {}"), status.error_message()));
  }
  return UBNSClientResult::success();
}

UBNSClientResult UBNSgRPCClient::update_bucket_request(const ubdb::v1::UpdateBucketEntryRequest& req)
{
  ::grpc::ClientContext context;
  ubdb::v1::UpdateBucketEntryResponse resp;
  auto status = stub_->UpdateBucketEntry(&context, req, &resp);
  // XXX precise error handling.
  if (!status.ok()) {
    return UBNSClientResult::error(ERR_INTERNAL_ERROR, fmt::format(FMT_STRING("gRPC error: {}"), status.error_message()));
  }
  return UBNSClientResult::success();
}

bool UBNSClientImpl::init(CephContext* cct, const std::string& grpc_uri)
{
  ceph_assert(cct != nullptr);

  auto& conf = cct->_conf;

  // Set up the configuration observer.
  config_obs_.init(cct);

  cluster_id_ = cct->_conf->rgw_ubns_cluster_id;

  // Empty grpc_uri (the default) means use the configuration value.
  auto uri = grpc_uri.empty() ? conf->rgw_ubns_grpc_uri : grpc_uri;

  if (conf->rgw_ubns_grpc_mtls_enabled) {
    if (!set_mtls_channel(cct)) {
      ldout(cct, 0) << "UBNS: Failed to create initial mTLS gRPC channel" << dendl;
      return false;
    }
  } else {
    if (!set_channel_uri(cct, uri)) {
      // This is unlikely, but no gRPC channel in gRPC mode is a critical error.
      ldout(cct, 0) << "UBNS: Failed to create initial gRPC channel" << dendl;
      return false;
    }
  }
  return true;
}

void UBNSClientImpl::shutdown()
{
  // The gRPC channel shutdown will be handled by the unique_ptr, on
  // destruction.
}

std::optional<UBNSgRPCClient> UBNSClientImpl::safe_get_client(const DoutPrefixProvider* dpp)
{
  UBNSgRPCClient client {};
  std::shared_lock<std::shared_mutex> g(m_channel_);
  // Quick confidence check of channel_.
  if (!channel_) {
    ldpp_dout(dpp, 0) << "Unset gRPC channel" << dendl;
    return std::nullopt;
  }
  client.set_stub(channel_);
  return std::make_optional(std::move(client));
}

UBNSClientResult UBNSClientImpl::add_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& cluster_id, const std::string& owner)
{
  ldpp_dout(dpp, 20) << __func__ << dendl;
  auto client = safe_get_client(dpp);
  if (!client) {
    return UBNSClientResult::error(ERR_INTERNAL_ERROR, "Internal error (could not fetch gRPC client)");
  }
  ldpp_dout(dpp, 1) << "UBNS: sending gRPC AddBucketRequest" << dendl;
  ubdb::v1::AddBucketEntryRequest req;
  req.set_bucket(bucket_name);
  req.set_cluster(cluster_id);
  req.set_owner(owner);
  return client->add_bucket_request(req);
}

UBNSClientResult UBNSClientImpl::delete_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& cluster_id)
{
  ldpp_dout(dpp, 20) << __func__ << dendl;
  auto client = safe_get_client(dpp);
  if (!client) {
    return UBNSClientResult::error(ERR_INTERNAL_ERROR, "Internal error (could not fetch gRPC client)");
  }
  ldpp_dout(dpp, 1) << "UBNS: sending gRPC DeleteBucketRequest" << dendl;
  ubdb::v1::DeleteBucketEntryRequest req;
  req.set_bucket(bucket_name);
  req.set_cluster(cluster_id);
  return client->delete_bucket_request(req);
}

UBNSClientResult UBNSClientImpl::update_bucket_entry(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& cluster_id, UBNSBucketUpdateState state)
{
  ldpp_dout(dpp, 20) << __func__ << dendl;
  auto client = safe_get_client(dpp);
  if (!client) {
    return UBNSClientResult::error(ERR_INTERNAL_ERROR, "Internal error (could not fetch gRPC client)");
  }
  ldpp_dout(dpp, 1) << "UBNS: sending gRPC UpdateBucketRequest" << dendl;
  ubdb::v1::UpdateBucketEntryRequest req;
  req.set_bucket(bucket_name);
  req.set_cluster(cluster_id);
  ubdb::v1::BucketState rpc_state;
  switch (state) {
  case rgw::UBNSBucketUpdateState::Unspecified:
    rpc_state = ubdb::v1::BucketState::BUCKET_STATE_UNSPECIFIED;
    break;
  case rgw::UBNSBucketUpdateState::Created:
    rpc_state = ubdb::v1::BucketState::BUCKET_STATE_CREATED;
    break;
  case rgw::UBNSBucketUpdateState::Deleting:
    rpc_state = ubdb::v1::BucketState::BUCKET_STATE_DELETING;
    break;
  }
  req.set_state(rpc_state);
  return client->update_bucket_request(req);
}

grpc::ChannelArguments UBNSClientImpl::get_default_channel_args(CephContext* const cct)
{
  grpc::ChannelArguments args;

  // Set our default backoff parameters. These are runtime-alterable.
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, cct->_conf->rgw_ubns_grpc_arg_initial_reconnect_backoff_ms);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, cct->_conf->rgw_ubns_grpc_arg_max_reconnect_backoff_ms);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, cct->_conf->rgw_ubns_grpc_arg_min_reconnect_backoff_ms);
  ldout(cct, 20) << fmt::format(FMT_STRING("HandoffHelperImpl::{}: reconnect_backoff(ms): initial/min/max={}/{}/{}"),
      __func__,
      cct->_conf->rgw_ubns_grpc_arg_initial_reconnect_backoff_ms,
      cct->_conf->rgw_ubns_grpc_arg_min_reconnect_backoff_ms,
      cct->_conf->rgw_ubns_grpc_arg_max_reconnect_backoff_ms)
                 << dendl;

  return grpc::ChannelArguments();
}

bool UBNSClientImpl::set_channel_uri(CephContext* const cct, const std::string& new_uri)
{
  std::unique_lock<chan_lock_t> g(m_channel_);
  if (!channel_args_) {
    auto args = get_default_channel_args(cct);
    // Don't use set_channel_args(), which takes lock m_channel_.
    channel_args_ = std::make_optional(std::move(args));
  }
  // grpc::InsecureChannelCredentials() is appropriate here. For a TLS-enabled
  // channel, see set_mtls_channel().
  auto new_channel = grpc::CreateCustomChannel(new_uri, grpc::InsecureChannelCredentials(), *channel_args_);
  if (!new_channel) {
    ldout(cct, 0) << "UBNSClientImpl::set_channel_uri(): ERROR: Failed to create new gRPC channel " << new_uri << dendl;
    return false;
  } else {
    ldout(cct, 1) << "UBNSClientImpl::set_channel_uri(" << new_uri << ") success" << dendl;
    channel_ = std::move(new_channel);
    channel_uri_ = new_uri;
    return true;
  }
}

// Given a filename, return the contents, or nullopt on failure.
static std::optional<std::string> load_credential_from_file(CephContext* const cct, const std::string& descr, const std::string& path)
{
  ldout(cct, 0) << fmt::format(FMT_STRING("{}: Load credential '{}' from file '{}'"), __func__, descr, path) << dendl;
  try {
    std::ifstream f(path, std::ios::in | std::ios::binary);
    std::string buffer;
    f.exceptions(std::ifstream::failbit | std::ifstream::badbit);
    f.seekg(0, std::ios::end);
    buffer.resize(f.tellg());
    f.seekg(0, std::ios::beg);
    f.read(&buffer[0], buffer.size());
    f.close();
    return buffer;
  } catch (std::ifstream::failure& e) {
    ldout(cct, 0) << fmt::format(FMT_STRING("{}: ERROR: Failed to load {} from file '{}': {}"), __func__, descr, path, e.what()) << dendl;
    return std::nullopt;
  }
}

bool UBNSClientImpl::set_mtls_channel(CephContext* const cct)
{
  auto& conf = cct->_conf;
  std::string new_uri = conf->rgw_ubns_grpc_uri;

  std::unique_lock<chan_lock_t> g(m_channel_);
  if (!channel_args_) {
    auto args = get_default_channel_args(cct);
    // Don't use set_channel_args(), which takes lock m_channel_.
    channel_args_ = std::make_optional(std::move(args));
  }

  // gRPC authentication: See https://grpc.io/docs/guides/auth/ .

  auto cred_options = grpc::SslCredentialsOptions();
  auto ca_cert = load_credential_from_file(cct, "CA cert", conf->rgw_ubns_grpc_mtls_ca_cert_file);
  if (!ca_cert) {
    return false;
  }
  cred_options.pem_root_certs = *ca_cert;
  auto client_cert = load_credential_from_file(cct, "Client cert", conf->rgw_ubns_grpc_mtls_client_cert_file);
  if (!client_cert) {
    return false;
  }
  cred_options.pem_cert_chain = *client_cert;
  auto client_key = load_credential_from_file(cct, "Client key", conf->rgw_ubns_grpc_mtls_client_key_file);
  if (!client_key) {
    return false;
  }
  cred_options.pem_private_key = *client_key;
  auto channel_creds = grpc::SslCredentials(cred_options);

  auto new_channel = grpc::CreateCustomChannel(new_uri, channel_creds, *channel_args_);
  if (!new_channel) {
    ldout(cct, 0) << __func__ << ": ERROR: Failed to create new gRPC channel for uri a" << new_uri << dendl;
    return false;
  } else {
    ldout(cct, 1) << "UBNS: " << __func__ << "(" << new_uri << ") success" << dendl;
    channel_ = std::move(new_channel);
    channel_uri_ = new_uri;
    return true;
  }

  return false;
}

} // namespace rgw
