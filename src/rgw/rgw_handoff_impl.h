/**
 * @file rgw_handoff_impl.h
 * @author André Lucas (alucas@akamai.com)
 * @brief Handoff declarations involving gRPC.
 * @version 0.1
 * @date 2023-11-10
 *
 * @copyright Copyright (c) 2023
 *
 * Declarations for HandoffHelperImpl and related classes.
 *
 * TRY REALLY HARD to not include this anywhere except handoff.cc and
 * handoff_impl.cc. In particular, don't add it to rgw_handoff.h no matter how
 * tempting that seems.
 *
 * This file pulls in the gRPC headers and we don't want that everywhere.
 */

#ifndef RGW_HANDOFF_IMPL_H
#define RGW_HANDOFF_IMPL_H

#include "acconfig.h"

#include <fmt/format.h>
#include <functional>
#include <iosfwd>
#include <shared_mutex>
#include <string>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>

#include "common/async/yield_context.h"
#include "common/ceph_context.h"
#include "common/config_obs.h"
#include "common/dout.h"
#include "rgw/rgw_common.h"

#include "rgw/auth/v1/auth.grpc.pb.h"
using namespace ::rgw::auth::v1;

#include "rgw_handoff.h"

namespace rgw {

/****************************************************************************/

/**
 * @brief Implement DoutPrefixPipe for a simple prefix string.
 *
 * To add an additional string (which will be followed by ": ") to the
 * existing log prefix, use:
 *
 * ```
 * auto hdpp = HandoffDoutPrefixPipe(*dpp_in, foo);
 * auto dpp = &hdpp;
 * ```
 *
 * It will save time to create a DoutPrefixProvider*, as demonstrated by the
 * unit tests it's quite jarring to have to use &dpp.
 */
class HandoffDoutPrefixPipe : public DoutPrefixPipe {
  const std::string prefix_;

public:
  HandoffDoutPrefixPipe(const DoutPrefixProvider& dpp, const std::string& prefix)
      : DoutPrefixPipe { dpp }
      , prefix_ { fmt::format("{}: ", prefix) }
  {
  }
  virtual void add_prefix(std::ostream& out) const override final
  {
    out << prefix_;
  }
};

/**
 * @brief Add request state as a prefix to the log message. This should be
 * used to help support engineers correlate log messages.
 *
 * Pass in the request state.
 *
 * ```
 * auto hdpp = HandoffDoutStateProvider(*dpp_in, s);
 * auto dpp = &hdpp;
 * ```
 */
class HandoffDoutStateProvider : public HandoffDoutPrefixPipe {

public:
  /**
   * @brief Construct a new Handoff Dout Pipe Provider object with an existing
   * provider and the request state.
   *
   * Use our HandoffDoutPrefixPipe implementation for implementation.
   *
   * @param dpp An existing DoutPrefixProvider reference.
   * @param s The request state.
   */
  HandoffDoutStateProvider(const DoutPrefixProvider& dpp, const req_state* s)
      : HandoffDoutPrefixPipe {
        dpp, fmt::format("HandoffEngine trans_id={}", s->trans_id)
      } {};
};

/****************************************************************************/

/**
 * @brief The result of parsing the HTTP response from the Authenticator service.
 *
 * Used by the HTTP arm of auth() to encapsulate the various possible results
 * from parsing the Authenticator's JSON.
 */
class HandoffHTTPVerifyResult {
  int result_;
  long http_code_;
  std::string query_url_;

public:
  HandoffHTTPVerifyResult()
      : result_ { -1 }
      , http_code_ { 0 }
      , query_url_ { "" }
  {
  }
  HandoffHTTPVerifyResult(int result, long http_code, std::string query_url = "")
      : result_ { result }
      , http_code_ { http_code }
      , query_url_ { query_url }
  {
  }
  // No copy or copy-assignment.
  HandoffHTTPVerifyResult(const HandoffHTTPVerifyResult& other) = delete;
  HandoffHTTPVerifyResult& operator=(const HandoffHTTPVerifyResult& other) = delete;
  // Trivial move and move-assignment.
  HandoffHTTPVerifyResult(HandoffHTTPVerifyResult&& other) = default;
  HandoffHTTPVerifyResult& operator=(HandoffHTTPVerifyResult&& other) = default;

  int result() const noexcept { return result_; }
  long http_code() const noexcept { return http_code_; }
  std::string query_url() const noexcept { return query_url_; }
};

/****************************************************************************/

/**
 * @brief gRPC client wrapper for rgw/auth/v1/AuthService.
 *
 * Very thin wrapper around the gRPC client. Construct with a channel to
 * create a stub. Call services via the corresponding methods, with sanitised
 * return values.
 */
class AuthServiceClient {
private:
  std::unique_ptr<AuthService::Stub> stub_;

public:
  /**
   * @brief Construct a new Auth Service Client object. You must use set_stub
   * before using any gRPC calls, or the object's behaviour is undefined.
   */
  AuthServiceClient() { }

  /**
   * @brief Construct a new AuthServiceClient object and initialise the gRPC
   * stub.
   *
   * @param channel pointer to the grpc::Channel object to be used.
   */
  explicit AuthServiceClient(std::shared_ptr<::grpc::Channel> channel)
      : stub_(AuthService::NewStub(channel))
  {
  }

  /**
   * @brief Set the gRPC stub for this object.
   *
   * @param channel the gRPC channel pointer.
   */
  void set_stub(std::shared_ptr<::grpc::Channel> channel)
  {
    stub_ = AuthService::NewStub(channel);
  }

  /**
   * @brief Call rgw::auth::v1::AuthService::Status() and return either the
   * status server_description message, or std::nullopt in case of error.
   *
   * @param req The (empty) request protobuf message.
   * @return std::optional<std::string> The server_description field of the
   * response, or std::nullopt on error.
   */
  std::optional<std::string> Status(const StatusRequest& req);

  /**
   * @brief Call rgw::auth::v1::AuthService::Auth() and cast the result to
   * HandoffAuthResult, suitable for HandoffHelperImpl::auth().
   *
   * On error, creates a HandoffAuthResult indicating a failure (500).
   *
   * Otherwise, calls parse_auth_response() to interpret the AuthResponse
   * returned by the RPC.
   *
   * The alternative to interpreting the request status and result object
   * would be either returning a bunch of connection status plus an
   * AuthResponse object, or throwing exceptions on error. This feels like an
   * easier API to use in our specific use case.
   *
   * @param req the filled-in authentication request protobuf
   * (rgw::auth::v1::AuthRequest).
   * @return HandoffAuthResult A completed auth result.
   */
  HandoffAuthResult Auth(const AuthRequest& req);

  /**
   * @brief Given an Auth service request and response, construct a matching
   * HandoffAuthResult ready to return from HandoffHelperImpl::auth().
   *
   * Called implicitly by Auth().
   *
   * All we have to do is map the AuthResponse \p code field (type
   * rgw::auth::v1::AuthCode) onto suitable HTTP-like response codes expected
   * by HandoffHelperImpl::auth(), and by extention HandoffHelper::auth(), and
   * finally by HandoffEngine::authenticate()).
   *
   * Only a response with code AUTH_CODE_OK results in a successful
   * authentication. Any other return value results in an authentication
   * failure. Note that RGW may try other authentication engines; this is
   * based on configuration and is not under the control of Handoff.
   *
   * @param req
   * @param resp
   * @return HandoffAuthResult
   */
  HandoffAuthResult parse_auth_response(const AuthRequest& req, const AuthResponse* resp);
};

/****************************************************************************/

/**
 * @brief Gathered information about an inflight request that we want to sent
 * to the Authentication service for verification.
 *
 * Normally these data are gathered later in the request and subject to
 * internal policies, acls etc. We're giving the Authentication service a
 * chance to see this information early.
 */
class AuthorizationParameters {

private:
  bool valid_;
  std::string method_;
  std::string bucket_name_;
  std::string object_key_name_;

  void valid_check() const
  {
    if (!valid()) {
      throw new std::runtime_error("AuthorizationParameters not valid");
    }
  }

public:
  AuthorizationParameters(const DoutPrefixProvider* dpp, const req_state* s) noexcept;

  // Standard copies and moves are fine.
  AuthorizationParameters(const AuthorizationParameters& other) = default;
  AuthorizationParameters& operator=(const AuthorizationParameters& other) = default;
  AuthorizationParameters(AuthorizationParameters&& other) = default;
  AuthorizationParameters& operator=(AuthorizationParameters&& other) = default;

  /**
   * @brief Return the validity of this AuthorizationParameters object.
   *
   * If at construction time the request was well-formed and contained
   * sufficient information to be used in an authorization request to the
   * Authenticator, return true.
   *
   * Otherwise, return false.
   *
   * @return true The request can be used as the source of an
   * authorization-enhanced authentication operation.
   * @return false The request cannot be used.
   */
  bool valid() const noexcept
  {
    return valid_;
  }
  /**
   * @brief Return the HTTP method for a valid request. Throw if valid() is
   * false.
   *
   * @return std::string The method.
   * @throw std::runtime_error if !valid().
   */
  std::string method() const
  {
    valid_check();
    return method_;
  }
  /**
   * @brief Return the bucket name for a valid request. Throw if valid() is
   * false.
   *
   * @return std::string The bucket name.
   * @throw std::runtime_error if !valid().
   */
  std::string bucket_name() const
  {
    valid_check();
    return bucket_name_;
  }
  /**
   * @brief Return the object key name for a valid request. Throw if valid()
   * is false.
   *
   * @return std::string The object key name.
   * @throw std::runtime_error if !valid().
   */
  std::string object_key_name() const
  {
    valid_check();
    return object_key_name_;
  }

  /**
   * @brief Convert this AuthorizationParameters object to string form.
   *
   * Note we don't dump the object key name - this might be a large string,
   * might be full of invalid characters, or might be private.
   *
   * @return std::string A string representation of the object. Works fine for
   * objects in the invalid state; this call is always safe.
   */
  std::string to_string() const noexcept;

  friend std::ostream& operator<<(std::ostream& os, const AuthorizationParameters& ep);
};

std::ostream& operator<<(std::ostream& os, const AuthorizationParameters& ep);

/****************************************************************************/

template <typename T>
class HandoffConfigObserver : public md_config_obs_t {
public:
  explicit HandoffConfigObserver(T& helper)
      : helper_(helper)
  {
  }

  ~HandoffConfigObserver()
  {
    if (cct_) {
      cct_->_conf.remove_observer(this);
    }
  }

  void init(CephContext* cct)
  {
    cct_ = cct;
    cct_->_conf.add_observer(this);
  }

  // Config observer. See notes in src/common/config_obs.h and for
  // ceph::md_config_obs_impl.
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
      const std::set<std::string>& changed) override;

private:
  T& helper_;
  CephContext* cct_;
};

/****************************************************************************/

/**
 * @brief Support class for 'handoff' authentication.
 *
 * Used by rgw::auth::s3::HandoffEngine to implement authentication via an
 * external Authenticator Service.
 *
 * In gRPC mode, holds long-lived state.
 */
class HandoffHelperImpl {

public:
  // Signature of the alternative verify function,  used only for testing.
  using HTTPVerifyFunc = std::function<HandoffHTTPVerifyResult(const DoutPrefixProvider*, const std::string&, ceph::bufferlist*, optional_yield)>;
  using chan_lock_t = std::shared_mutex;

private:
  // Ceph configuration observer.
  HandoffConfigObserver<HandoffHelperImpl> config_obs_;

  const std::optional<HTTPVerifyFunc> http_verify_func_;
  rgw::sal::Store* store_;

  // The gRPC channel pointer needs to be behind a mutex.
  std::shared_mutex m_channel_;
  std::shared_ptr<grpc::Channel> channel_;
  grpc::ChannelArguments channel_args_;
  std::string channel_uri_;

public:
  /**
   * @brief Construct a new HandoffHelperImpl object.
   *
   * This is the constructor to use for all except unit tests. Note no
   * persisted state is set up; that's done by calling init().
   */
  HandoffHelperImpl()
      : config_obs_(*this)
  {
  }

  ~HandoffHelperImpl() = default;

  /**
   * @brief Construct a new Handoff Helper object with an alternative callout
   * mechanism. Used by test harnesses.
   *
   * @param v A function to replace the HTTP client callout. This must mimic
   * the inputs and outputs of the \p http_verify_standard() function.
   */
  HandoffHelperImpl(HTTPVerifyFunc v)
      : config_obs_ { *this }
      , http_verify_func_ { v }
  {
  }

  /**
   * @brief Initialise any long-lived state for this engine.
   * @param cct Pointer to the Ceph context.
   * @param store Pointer to the sal::Store object.
   * @param grpc_uri Optional URI for the gRPC server. If empty (the default),
   * config value rgw_handoff_grpc_uri is used.
   * @return 0 on success, otherwise failure.
   *
   * Store long-lived state.
   *
   * The \p store pointer isn't used at this time.
   *
   * In gRPC mode, a grpc::Channel is created and stored on the object for
   * later use. This will manage the persistent connection(s) for all gRPC
   * communications.
   */
  int init(CephContext* const cct, rgw::sal::Store* store, const std::string& grpc_uri = "");

  /**
   * @brief Set the gRPC channel URI.
   *
   * This is used by init() and by the config observer.
   *
   * @param grpc_uri
   * @return true on success.
   * @return false on failure.
   */
  bool set_channel_uri(CephContext* const cct, const std::string& grpc_uri);

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
   * - Depending on configuration, call either the gRPC arm (_grpc_auth()) or
   *   the HTTP arm (_http_auth()) and return the result.
   *
   */
  HandoffAuthResult auth(const DoutPrefixProvider* dpp,
      const std::string_view& session_token,
      const std::string_view& access_key_id,
      const std::string_view& string_to_sign,
      const std::string_view& signature,
      const req_state* const s,
      optional_yield y);

  /**
   * @brief Implement the gRPC arm of auth().
   *
   * @param dpp DoutPrefixProvider.
   * @param auth The authorization header, which may have been synthesized.
   * @param authorization_param Authorization parameters, if required.
   * @param session_token Unused by Handoff.
   * @param access_key_id The S3 access key.
   * @param string_to_sign The canonicalised S3 signature input.
   * @param signature The transaction signature provided by the user.
   * @param s Pointer to the req_state.
   * @param y An optional yield token.
   * @return HandoffAuthResult The authentication result.
   *
   * Implement a Handoff authentication request using gRPC.
   *
   * - Fill in the provided information in the request protobuf
   *   (rgw::auth::v1::AuthRequest).
   *
   * - If authorization parameters are provided, fill those in in the protobuf
   *   as well.
   *
   * - Send the request using an instance of rgw::AuthServiceClient. note that
   *   AuthServiceClient::Auth() handles the translation of the response code
   *   into a code suitable to be returned to RGW as the result of the engine
   *   authenticate() call.
   *
   * - If the gRPC request itself failed, log the error and return 'access
   *   denied'.
   *
   * - Log the authentication request's success or failure, and return the
   *   result from AuthServiceClient::Auth().
   */
  HandoffAuthResult _grpc_auth(const DoutPrefixProvider* dpp,
      const std::string& auth,
      const std::optional<AuthorizationParameters>& authorization_param,
      const std::string_view& session_token,
      const std::string_view& access_key_id,
      const std::string_view& string_to_sign,
      const std::string_view& signature,
      const req_state* const s,
      optional_yield y);

  /**
   * @brief Set custom gRPC channel arguments. Intended for testing.
   *
   * Keep this simple. If you set vtable args you'll need to worry about the
   * lifetime of those is longer than the HandoffHelperImpl object that will
   * store a copy of the ChannelArguments object.
   *
   * @param args A populated grpc::ChannelArguments object.
   */
  void set_channel_args(const grpc::ChannelArguments& args)
  {
    std::unique_lock l { m_channel_ };
    channel_args_ = args;
  }

  /**
   * @brief Implement the HTTP arm of auth().
   *
   * Implement a Handoff authentication request using REST.
   *
   * - Construct a JSON payload for the authenticator in the
   *   prescribed format.
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
   *
   * @param dpp DoutPrefixProvider.
   * @param auth The authorization header, which may have been synthesized.
   * @param authorization_param Authorization parameters, if required.
   * @param session_token Unused by Handoff.
   * @param access_key_id The S3 access key.
   * @param string_to_sign The canonicalised S3 signature input.
   * @param signature The transaction signature provided by the user.
   * @param s Pointer to the req_state.
   * @param y An optional yield token.
   * @return HandoffAuthResult The authentication result.
   */
  HandoffAuthResult _http_auth(const DoutPrefixProvider* dpp,
      const std::string& auth,
      const std::optional<AuthorizationParameters>& authorization_param,
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

}; // class HandoffHelperImpl

} /* namespace rgw */

#endif // RGW_HANDOFF_IMPL_H
