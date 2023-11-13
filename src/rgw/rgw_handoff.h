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
 */

#ifndef RGW_HANDOFF_H
#define RGW_HANDOFF_H

#include "acconfig.h"

#include <fmt/format.h>
#include <functional>
#include <iosfwd>
#include <string>

#include "common/async/yield_context.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "rgw/rgw_common.h"

namespace rgw {

/**
 * @brief Return type of the HandoffHelper auth() method.
 *
 * Encapsulates either the return values we need to continue on successful
 * authentication, or a failure code.
 */
class HandoffAuthResult {
  std::string userid_ = "";
  int errorcode_ = 0;
  std::string message_ = "";
  bool is_err_ = false;

public:
  /// @brief Construct a success-type result. \p message is
  /// human-readable status.
  HandoffAuthResult(const std::string& userid, const std::string& message)
      : userid_ { userid }
      , message_ { message }
      , is_err_ { false } {};
  /// @brief Construct a failure-type result with an error code.
  /// \p message is human-readable status.
  HandoffAuthResult(int errorcode, const std::string& message)
      : errorcode_ { errorcode }
      , message_ { message }
      , is_err_ { true } {};

  bool is_err() const noexcept { return is_err_; }
  bool is_ok() const noexcept { return !is_err_; }
  int code() const noexcept { return errorcode_; }
  std::string message() const noexcept { return message_; }

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
      return fmt::format("error={} message={}", errorcode_, message_);
    } else {
      return fmt::format("userid='{}' message={}", userid_, message_);
    }
  }
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
   * Simply calls the HandoffHelperImpl::auth() and returns its result.
   */
  HandoffAuthResult auth(const DoutPrefixProvider* dpp,
      const std::string_view& session_token,
      const std::string_view& access_key_id,
      const std::string_view& string_to_sign,
      const std::string_view& signature,
      const req_state* const s,
      optional_yield y);
};

} /* namespace rgw */

#endif /* RGW_HANDOFF_H */
