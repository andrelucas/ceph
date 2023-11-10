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

/**
 * @brief Support class for 'handoff' authentication.
 *
 * Used by rgw::auth::s3::HandoffEngine to implement authentication via an
 * external REST service.
 *
 * In gRPC mode, holds common state.
 */
class HandoffHelperImpl {

public:
  // Signature of the alternative verify function,  used only for testing.
  using VerifyFunc = std::function<HandoffVerifyResult(const DoutPrefixProvider*, const std::string&, ceph::bufferlist*, optional_yield)>;

private:
  const std::optional<VerifyFunc> verify_func_;
  rgw::sal::Store* store_;

public:
  HandoffHelperImpl() { }

  ~HandoffHelperImpl() = default;

  /**
   * @brief Construct a new Handoff Helper object with an alternative callout
   * mechanism. Used by test harnesses.
   *
   * @param v A function to replace the HTTP client callout. This must mimic
   * the inputs and outputs of the \p verify_standard() function.
   */
  HandoffHelperImpl(VerifyFunc v)
      : verify_func_ { v }
  {
  }

  /**
   * @brief Initialise any long-lived state for this engine.
   * @param cct Pointer to the Ceph context.
   * @param store Pointer to the sal::Store object.
   * @return 0 on success, otherwise failure.
   *
   * Currently a placeholder, there's no long-lived state at this time.
   */
  int init(CephContext* const cct, rgw::sal::Store* store);

  /**
   * @brief Authenticate the transaction using the Handoff engine.
   * @param dpp Debug prefix provider. Points to the Ceph context.
   * @param session_token Unused by Handoff.
   * @param access_key_id The S3 access key.
   * @param string_to_sign The canonicalised S3 signature input.
   * @param signature The transaction signature provided by the user.
   * @param s Pointer to the req_state.
   * @param y An optional yield token.
   * @return A HandofAuthResult encapsulating a return error code and any
   * parameters necessary to continue processing the request, e.g. the uid
   * associated with the access key.
   *
   * Perform request authentication via the external authenticator.
   *
   * There is a mechanism for a test harness to replace the HTTP client
   * portion of this function. Here we'll assume we're using the HTTP client
   * to authenticate.
   *
   * - Extract the Authorization header from the environment. This will be
   *   necessary to validate a v4 signature because we need some fields (date,
   *   region, service, request type) for step 2 of the signature process.
   *
   * - If the Authorization header is absent, attempt to extract the relevant
   *   information from query parameters to synthesize an Authorization
   *   header. This is to support presigned URLs.
   *
   * - If the header indicates AWS Signature V2 authentication, but V2 is
   *   disabled via configuration, return a failure immediately.
   *
   * - If required, introspect the request to obtain additional authentication
   *   parameters that might be required by the external authenticator.
   *
   * - Construct a JSON payload for the authenticator in the prescribed
   *   format.
   *
   * - At this point, call a test harness to perform authentication if one is
   *   configured. Otherwise...
   *
   * - Fetch the authenticator URI from the context. This can't be trivially
   *   cached, as we want to support changing it at runtime. However, future
   *   enhancements may perform some time-based caching if performance
   *   profiling shows this is a problem.
   *
   * - Append '/verify' to the authenticator URI.
   *
   * - Send the request to the authenticator using an RGWHTTPTransceiver. We
   *   need the transceiver version as we'll be both sending a POST request
   *   and reading the response body. (This is cribbed from the Keystone
   *   code.)
   *
   * - If the request send itself fails (we'll handle failure return codes
   *   presently), return EACCES immediately.
   *
   * - Parse the JSON response to obtain the human-readable message field,
   *   even if the authentication response is a failure.
   *
   * - If the request returned 200, return success.
   *
   * - If the request returned 401, return ERR_SIGNATURE_NO_MATCH.
   *
   * - If the request returned 404, return ERR_INVALID_ACCESS_KEY.
   *
   * - If the request returned any other code, return EACCES.
   */
  HandoffAuthResult auth(const DoutPrefixProvider* dpp,
      const std::string_view& session_token,
      const std::string_view& access_key_id,
      const std::string_view& string_to_sign,
      const std::string_view& signature,
      const req_state* const s,
      optional_yield y);

  /**
   * @brief Construct an Authorization header from the parsed query string
   * parameters.
   *
   * The Authorization header is a fairly concise way of sending a bunch of
   * bundled parameters to the Authenticator. So if (as would be the case with
   * a presigned URL) we don't get an Authorization header, see if we can
   * synthesize one from the query parameters.
   *
   * This function first has to distinguish between v2 and v4 parameters
   * (normally v2 if no region is supplied, defaulting to us-east-1). Then it
   * has to parse the completely distinct parameters for each version into a
   * v2 or v4 Authorization: header, via synthesize_v2_header() or
   * synthesize_v4_header() respectively.
   *
   * @param dpp DoutPrefixProvider.
   * @param s The request.
   * @return std::optional<std::string> The header on success, or std::nullopt
   * on any failure.
   */
  std::optional<std::string> synthesize_auth_header(
      const DoutPrefixProvider* dpp,
      const req_state* s);

  /**
   * @brief Assuming an already-parsed (via synthesize_auth_header) presigned
   * header URL, check that the given expiry time has not expired. Note that
   * in v17.2.6, this won't get called - RGW checks the expiry time before
   * even calling our authentication engine.
   *
   * Fail closed - if we can't parse the parameters to check, assume we're not
   * authenticated.
   *
   * The fields are version-specific. For the v2-ish URLs (no region
   * specified), we're given an expiry unix time to compare against. For the
   * v4-type URLs (region specified), we're given a start time and a delta in
   * seconds.
   *
   * @param dpp DoutPrefixProvider.
   * @param s The request.
   * @param now The current UNIX time (seconds since the epoch).
   * @return true The request has not expired
   * @return false The request has expired, or a check was not possible
   */
  bool valid_presigned_time(const DoutPrefixProvider* dpp, const req_state* s, time_t now);

  /**
   * @brief Return true if the supplied credential looks like an Extended
   * Access Key.
   *
   * The EAK format is specified to have a small set of known prefix strings,
   * to make them easy to detect. The prefixes are case-significant.
   *
   * @param access_key_id
   * @return true The credential has a recognised EAK prefix.
   * @return false The credential is a regular access key.
   */
  bool is_eak_credential(const std::string_view access_key_id);
};

} /* namespace rgw */

#endif // RGW_HANDOFF_IMPL_H
