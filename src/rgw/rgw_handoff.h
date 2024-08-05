// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/**
 * @file rgw_handoff.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief 'Handoff' S3 authentication engine.
 * @version 0.1
 * @date 2023-07-04
 *
 * Persistent 'helper' class for the Handoff authentication engine for S3.
 * This allows us to keep items such as a pointer to the store abstraction
 * layer and a gRPC channel around between requests.
 *
 * HandoffHelper simply wraps HandoffHelperImpl. Keep the number of classes in
 * this file to a strict minimum - most should be in rgw_handoff_impl.{h,cc}.
 *
 * DO NOT INCLUDE "rgw_handoff_impl.h" from here!
 */

#pragma once

#include <fmt/format.h>
#include <iosfwd>
#include <string>

#include <boost/container/flat_map.hpp>

#include "common/async/yield_context.h"
#include "common/dout.h"
#include "rgw/driver/rados/rgw_rados.h"
#include "rgw/rgw_common.h"

namespace rgw {

/**
 * @brief Return type of the HandoffHelper auth() method.
 *
 * Encapsulates either the return values we need to continue on successful
 * authentication, or a failure code.
 */
class HandoffAuthResult {
public:
  /**
   * @brief Classification of error-type results, to help with logging.
   */
  enum class error_type {
    NO_ERROR,
    TRANSPORT_ERROR,
    AUTH_ERROR,
    INTERNAL_ERROR,
  };

public:
  /**
   * @brief Construct a success-type result for a regular user.
   *
   * @param userid The user ID associated with the request.
   * @param message human-readable status.
   */
  HandoffAuthResult(const std::string& userid, const std::string& message)
      : userid_ { userid }
      , message_ { message }
      , is_err_ { false }
      , err_type_ { error_type::NO_ERROR } {};

  /**
   * @brief Construct a success-type result for a regular user, with an
   * embedded signing key used to support chunked uploads.
   *
   * @param userid The user ID associated with the request.
   * @param message human-readable status.
   * @param signing_key The signing key associated with the request, an
   * HMAC-SHA256 value as raw bytes.
   */
  HandoffAuthResult(const std::string& userid, const std::string& message,
      const std::vector<uint8_t>& signing_key)
      : userid_ { userid }
      , signing_key_ { signing_key }
      , message_ { message }
      , is_err_ { false }
      , err_type_ { error_type::NO_ERROR } {};

  /**
   * @brief Construct a failure-type result.
   *
   * \p message is human-readable.\p errorcode is one of the codes in
   * rgw_common.cc, array rgw_http_s3_errors. If we don't map exactly, it's
   * most likely because those error codes don't match the HTTP return code we
   * want.
   *
   * @param errorcode The RGW S3 error code.
   * @param message human-readable status.
   * @param err_type The error type enum, which will help give better error
   * log messages.
   */
  HandoffAuthResult(unsigned int errorcode, const std::string &message,
                    error_type err_type = error_type::AUTH_ERROR)
      : errorcode_{errorcode}, message_{message}, is_err_{true},
        err_type_{err_type} {};

  bool is_err() const noexcept { return is_err_; }
  bool is_ok() const noexcept { return !is_err_; }
  error_type err_type() const noexcept { return err_type_; }
  unsigned int code() const noexcept { return errorcode_; }
  std::string message() const noexcept { return message_; }
  /**
   * @brief Return the signing key, if any.
   *
   * A signing key is nonempty for chunked requests, and is empty otherwise.
   *
   * @return std::string the signing key, encoded as raw bytes.
   */
  std::optional<std::vector<uint8_t>> signing_key() const noexcept
  {
    return signing_key_;
  }
  bool has_signing_key() { return signing_key_.has_value(); }
  /**
   * @brief Set the signing key.
   *
   * @param key The binary signing key.
   */
  void set_signing_key(const std::vector<uint8_t> key) { signing_key_ = key; }

  /// @brief Return the user ID for a success result. Throw EACCES on
  /// failure.
  ///
  /// This is to catch erroneous use of userid(). It will probably get
  /// thrown all the way up to rgw::auth::Strategy::authenticate().
  std::string userid() const
  {
    if (is_err()) {
      throw -EACCES;
    }
    return userid_;
  }

  std::string to_string() const noexcept
  {
    if (is_err()) {
      return fmt::format(FMT_STRING("error={} message={}"), errorcode_, message_);
    } else {
      return fmt::format(FMT_STRING("userid='{}' message={}"), userid_, message_);
    }
  }

  friend std::ostream& operator<<(std::ostream& os, const HandoffAuthResult& ep);

private:
  std::string userid_ = "";
  std::optional<std::vector<uint8_t>> signing_key_;
  unsigned int errorcode_ = 0;
  std::string message_ = "";
  bool is_err_ = false;
  error_type err_type_ = error_type::NO_ERROR;
};

class HandoffHelperImpl; // Forward declaration.

/**
 * @brief Support class for 'handoff' authentication.
 *
 * Used by rgw::auth::s3::HandoffEngine to implement authentication via an
 * external REST service. Note this is essentially a wrapper class - the work
 * is all done in rgw::HandoffHelperImpl, to keep the gRPC headers away from
 * the rest of RGW.
 */
class HandoffHelper {

private:
  /* There's some trouble taken to make a smart pointer to an incomplete
   * object work properly. See notes around the destructor declaration and
   * definition, it's subtle.
   */
  std::unique_ptr<HandoffHelperImpl> impl_;

public:
  /*
   * Implementation note: We need to implement the constructor(s) and
   * destructor when we know the size of HandoffHelperImpl. This means we
   * implement in the .cc file, which _does_ include the impl header file.
   * *Don't* include the impl header file in this .h, and don't switch to
   * using the default implementation - it won't compile.
   */

  HandoffHelper();

  ~HandoffHelper();

  /**
   * @brief Initialise any long-lived state for this engine.
   * @param cct Pointer to the Ceph context.
   * @param store Pointer to the sal::Store object.
   * @return 0 on success, otherwise failure.
   *
   * Initialise the long-lived object. Calls HandoffHelperImpl::init() and
   * returns its result.
   */
  int init(CephContext* const cct, rgw::sal::Driver* store);

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
   * Simply calls the HandoffHelperImpl::auth() and returns its result.
   */
  HandoffAuthResult auth(const DoutPrefixProvider* dpp,
      const std::string_view& session_token,
      const std::string_view& access_key_id,
      const std::string_view& string_to_sign,
      const std::string_view& signature,
      const req_state* const s,
      optional_yield y);

  HandoffAuthResult anonymous_authorize(const DoutPrefixProvider* dpp,
      const req_state* const s,
      optional_yield y);

  /**
   * @brief Authorize the operation via the external Authorizer.
   *
   * Call out to the external Authorizer to verify the operation, using a
   * combination of the req_state, our saved authorization state (if any), and
   * the operation code (e.g. rgw::IAM::GetObject).
   *
   * Frustratingly, \p s is contained in \op, but it's a protected member so
   * we provide it explicitly.
   *
   * \p s is non-const because we might have to modify it, e.g. by loading
   * bucket or object tags that live in the environment, or by setting
   * additional authz state.
   *
   * @param op The RGWOp-subclass object pointer.
   * @param s The req_state object.
   * @param operation The operation code.
   * @param y optional yield (will likely be ignored).
   * @return int return code. 0 for success, <0 for error, typically -EACCES.
   */
  int verify_permission(const RGWOp* op, req_state* s,
      uint64_t operation, optional_yield y);

  /**
   * @brief Return true if anonymous authorization is enabled, false otherwise.
   *
   * @return true Anonymous authorization is enabled.
   * @return false Anonymous authorization is disabled.
   */
  bool anonymous_authorization_enabled() const;

  /**
   * @brief Return true if local authorization may be bypassed because we've
   * already authorized the request.
   *
   * Simply calls HandoffHelperImpl::local_authorization_bypass_allowed() and
   * returns the result.
   *
   * @param s The request.
   * @return true Local authorization may be bypassed.
   * @return false Local authorization MUST NOT be bypassed.
   */
  bool local_authorization_bypass_allowed(const req_state *s) const;

  /**
   * @brief Return true if Handoff is configured to disable *all* local
   * authorization checks in favour of external authorization.
   *
   * This is intended for use in subordinate functions of process_request() -
   * mostly rgw_process_authenticated() - to determine whether or not we
   * should attempt local authorization.
   *
   * @return true Local authorization is disabled.
   * @return false Local authorization is enabled and should not be bypassed.
   */
  bool disable_local_authorization() const;
};

/**
 * @brief Per-request state information for the Handoff authorization client.
 *
 * Try not to couple this too hard with the HandoffHelper. The moment you put
 * a std::shared_ptr<HandoffHelper> in here, it makes things much harder to
 * test. This should just be a carrier for authorization state.
 *
 * The most basic state it carries is whether or not Handoff Authorization is
 * even enabled. As a method on an object that always exists, it makes the
 * test for authz much easier to read:
 *
 * ```
 *  if (s->handoff_authz().enabled()) {
 *      ...
 *   } else {
 *     ...
 *   }
 * ```
 *
 * It has a constructior with a HandoffHelper pointer simply so it can call
 * methods on the helper to initialise itself. Other constructors will server
 * for unit tests.
 */
class HandoffAuthzState {
  bool enabled_ = false;
  std::string bucket_name_;
  std::string object_key_name_;

  struct AuthenticatorParameters {
    std::string canonical_user_id_;
    std::string user_arn_;
    std::optional<std::string> assuming_user_arn_;
    std::string account_arn_;
  }; // struct AuthenticatorParameters

  mutable AuthenticatorParameters authenticator_params_;

  bool bucket_tags_required_ = false;
  bool object_tags_required_ = false;

  boost::container::flat_map<std::string, std::string> bucket_tags_;
  boost::container::flat_map<std::string, std::string> object_tags_;

public:
  HandoffAuthzState() = delete;
  /// Construct explicitly (for tests).
  explicit HandoffAuthzState(bool enabled)
      : enabled_(enabled)
  {
  }

  /**
   * @brief Construct a new Handoff Authz State object using a HandoffHelper.
   *
   * Construct using an existing HandoffHelper. This keeps Handoff-specific
   * initialisation code out of rgw_process.cc.
   *
   * @param helper a pointer to the shared HandoffHelper object. May be nullptr!
   */
  explicit HandoffAuthzState(std::shared_ptr<HandoffHelper> helper);

  /// Return true if Handoff Authorization is enabled.

  /**
   * @brief Return true if Handoff Authorization is enabled.
   *
   * Note that disabled() is present too, and is just the negation of this
   * method. Having both means one can write the 'if' test that reads most
   * clearly.
   *
   * @return true Handoff Authorization is enabled.
   * @return false Handoff Authorization is disabled.
   */
  bool enabled() const noexcept { return enabled_; }

  /**
   * @brief Return true if Handoff Authorization is disabled.
   *
   * Just the negation of enabled() - having both means one can write the 'if'
   * test that reads most clearly.
   *
   * @return true Handoff Authorization is disabled.
   * @return false Handoff Authorization is enabled.
   */
  bool disabled() const noexcept { return !enabled_; }

  /**
   * @brief Return the bucket name.
   *
   * @return std::string The bucket name.
   */
  std::string bucket_name() const noexcept { return bucket_name_; }

  /**
   * @brief Set the bucket name.
   *
   * This lives in the HandoffAuthzState object in order to make the
   * authorization code easier to test. Without a 'safe' place for the bucket
   * name to be stored, we'd have to pass a req_state around with enough
   * support to fetch the object key name. That either means parsing URL
   * fields (which assumes that's even safe to do), or actually loading the
   * bucket which is completely impractical - the bucket is an
   * rgw::sal::Bucket instance, with a million pure virtual methods.
   *
   * So we just copy the string into the state object. Easy to do, easy to
   * test.
   *
   * @param name The bucket name.
   */
  void set_bucket_name(const std::string& name) noexcept { bucket_name_ = name; }

  /**
   * @brief Return the object key name.
   *
   * This lives in the HandoffAuthzState object in order to make the
   * authorization code easier to test. Without a 'safe' place for the object
   * key name to be stored, we'd have to pass a req_state around with enough
   * support to fetch the object key name. That either means parsing URL
   * fields (which assumes that's even safe to do), or actually loading the
   * object which is completely impractical - the object key is an
   * rgw::sal::Object instance, with a million pure virtual methods.
   *
   * So we just copy the string into the state object. Easy to do, easy to
   * test.
   *
   * @return std::string The object key name.
   */
  std::string object_key_name() const noexcept { return object_key_name_; }

  /**
   * @brief Set the object key name.
   *
   * @param name The object key name.
   */
  void set_object_key_name(const std::string& name) noexcept { object_key_name_ = name; }

  /**
   * @brief Preserve ID-related fields returned by the Authenticator.
   *
   * These are the fields in the authenticator.v1.AuthenticateResponse
   * message, which we'll replay to the Authorizer.
   *
   * We are doing some gymnastics here to deal with the fact that we'll
   * normally be dealing with a pointer to a member field (HandoffAuthzState)
   * of a const pointer to req_state, but we need to be able to modify the
   * authorization state. So we're using mutable fields to allow this, and
   * this method is marked const to allow it to be called. It sucks.
   *
   * @param canonical_user_id Corresponding field in AuthenticateResponse.
   * @param user_arn Corresponding field in AuthenticateResponse.
   * @param assuming_user_arn Corresponding field in AuthenticateResponse.
   * @param account_arn Corresponding field in AuthenticateResponse.
   */
  void set_authenticator_id_fields(const std::string& canonical_user_id,
      const std::string& user_arn,
      const std::optional<std::string>& assuming_user_arn,
      const std::string& account_arn) const noexcept
  {
    authenticator_params_.canonical_user_id_ = canonical_user_id;
    authenticator_params_.user_arn_ = user_arn;
    authenticator_params_.assuming_user_arn_ = assuming_user_arn;
    authenticator_params_.account_arn_ = account_arn;
  }

  /**
   * @brief Return the canonical user ID.
   *
   * This is an Authenticator AuthenticateRESTResponse field that we reflect
   * back to the Authorizer.
   *
   * @return std::string The canonical user ID.
   */
  std::string canonical_user_id() const noexcept { return authenticator_params_.canonical_user_id_; }

  /**
   * @brief Return the user ARN.
   *
   * This is an Authenticator AuthenticateRESTResponse field that we reflect
   * back to the Authorizer.
   *
   * @return std::string The user ARN.
   */
  std::string user_arn() const noexcept { return authenticator_params_.user_arn_; }

  /**
   * @brief Return the assuming user ARN.
   *
   * This is an Authenticator AuthenticateRESTResponse field that we reflect
   * back to the Authorizer.
   *
   * @return std::optional<std::string> The assuming user ARN.
   */
  std::optional<std::string> assuming_user_arn() const noexcept { return authenticator_params_.assuming_user_arn_; }

  /**
   * @brief Return the account ARN.
   *
   * This is an Authenticator AuthenticateRESTResponse field that we reflect
   * back to the Authorizer.
   *
   * @return std::string The account ARN.
   */
  std::string account_arn() const noexcept { return authenticator_params_.account_arn_; }

  /**
   * @brief Return true if *any* extra data field must be provided.
   *
   * This must be updated if more extra data fields are added in the future.
   * It's how PopulateAuthorizeRequest() knows whether or not to include the
   * extra_data_provided and extra_data fields.
   *
   * @return true One or more piece of extra data is required.
   * @return false No extra data are required.
   */
  bool extra_data_required() const noexcept
  {
    return bucket_tags_required_ || object_tags_required_;
  }

  /**
   * @brief Return true if bucket tags are required.
   *
   * @return true Bucket tags are required.
   * @return false Bucket tags are not required.
   */
  bool bucket_tags_required() const noexcept { return bucket_tags_required_; }

  /**
   * @brief Set whether or not bucket tags are required.
   *
   * @param required true if bucket tags are required, false otherwise.
   */
  void set_bucket_tags_required(bool required) noexcept { bucket_tags_required_ = required; }

  /**
   * @brief Return true if object tags are required.
   *
   * @return true Object tags are required.
   * @return false Object tags are not required.
   */
  bool object_tags_required() const noexcept { return object_tags_required_; }

  /**
   * @brief Set whether or not object tags are required.
   *
   * @param required true if object tags are required, false otherwise.
   */
  void set_object_tags_required(bool required) noexcept { object_tags_required_ = required; }

  /**
   * @brief Set an individual entry in the bucket tags map.
   *
   * This is a regular map, so writing to a key that already exists will
   * replace it.
   *
   * @param key The bucket tag key.
   * @param value The bucket tag entry.
   */
  void set_bucket_tag_entry(const std::string& key, const std::string& value) noexcept
  {
    bucket_tags_[key] = value;
  }

  /**
   * @brief Get an individual bucket tag entry, if present.
   *
   * @param key The bucket tag key.
   * @return std::optional<std::string> The bucket tag entry value for \p key,
   * or std::nullopt if the key is not present.
   */
  std::optional<std::string> get_bucket_tag_entry(const std::string& key) const noexcept;

  /**
   * @brief Get the object tag entry object
   *
   * @param key The object tag key.
   * @return std::optional<std::string> The object tag entry value for \p key,
   * or std::nullopt if the key is not present.
   */
  std::optional<std::string> get_object_tag_entry(const std::string& key) const noexcept;

  /**
   * @brief Set an individual entry in the object tags map.
   *
   * This is a regular map, so writing to a key that already exists will
   * replace it.
   *
   * @param key The object tag key.
   * @param value The object tag entry.
   */
  void set_object_tag_entry(const std::string& key, const std::string& value) noexcept
  {
    object_tags_[key] = value;
  }

  /**
   * @brief Return a copy of the bucket tags map.
   *
   * @return const boost::container::flat_map<std::string, std::string>& The bucket tags map.
   */
  const boost::container::flat_map<std::string, std::string> bucket_tags() const noexcept
  {
    return bucket_tags_;
  }

  /**
   * @brief Return a copy of the object tags map.
   *
   * @return const boost::container::flat_map<std::string, std::string>& The object tags map.
   */
  const boost::container::flat_map<std::string, std::string> object_tags() const noexcept
  {
    return object_tags_;
  }

}; // class HandoffAuthzState

} /* namespace rgw */
