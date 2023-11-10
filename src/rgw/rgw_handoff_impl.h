/**
 * @file rgw_handoff_impl.h
 * @author André Lucas (alucas@akamai.com)
 * @brief Handoff declarations involving gRPC.
 * @version 0.1
 * @date 2023-11-10
 *
 * @copyright Copyright (c) 2023
 *
 * Only include gRPC headers here. We don't want gRPC headers being pulled
 * into the rest of RGW.
 */

#ifndef RGW_HANDOFF_IMPL_H
#define RGW_HANDOFF_IMPL_H

#include "acconfig.h"

#include <fmt/format.h>
#include <functional>
#include <iosfwd>
#include <string>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>

#include "common/async/yield_context.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "rgw/rgw_common.h"

#include "rgw/auth/v1/auth.grpc.pb.h"
using namespace ::rgw::auth::v1;

#include "rgw_handoff.h"

namespace rgw {

class AuthClient {
private:
  std::unique_ptr<AuthService::Stub> stub_;

public:
  AuthClient(std::shared_ptr<::grpc::Channel> channel)
      : stub_(AuthService::NewStub(channel))
  {
  }

  ~AuthClient();

  HandoffAuthResult Auth(const AuthRequest& req)
  {
    ::grpc::ClientContext context;
    AuthResponse resp;

    ::grpc::Status status = stub_->Auth(&context, req, &resp);
    // Check for an error from gRPC itself.
    if (!status.ok()) {
      return HandoffAuthResult(500, status.error_message());
    }
    return parse_auth_response(req, &resp);
  }

  HandoffAuthResult parse_auth_response(const AuthRequest& req, const AuthResponse* resp)
  {
    return HandoffAuthResult(500, "XXX NOTIMPL");
  }
};

AuthClient::~AuthClient() = default;

} /* namespace rgw */

#endif // RGW_HANDOFF_IMPL_H
