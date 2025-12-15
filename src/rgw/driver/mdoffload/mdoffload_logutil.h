/**
 * @file mdoffload_logutil.h
 * @author André Lucas (alucas@akamai.com)
 * @brief Logging-related utility functions for MDOffload driver.
 * @version 0.1
 * @date 2025-12-15
 *
 * @copyright Copyright (c) 2025
 *
 */

#pragma once

#include <string>
#include <fmt/format.h>

#include "rgw_common.h"
#include "rgw_sal.h"

/*****************************************************************************/

// fmtlib formatters for Ceph types. Some of these have to_str() and their own
// operator<<, but this way keeps things consistent.

template <>
struct fmt::formatter<rgw_obj_key> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const rgw_obj_key& k, FormatContext& ctx)
  {
    return format_to(ctx.out(), FMT_STRING("rgw_obj_key{{key='{}',instance='{}'}}"), k.name, k.instance);
  }
};

template <>
struct fmt::formatter<rgw_user> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const rgw_user& u, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    return fmt::format_to(ctx.out(), "rgw_user{{tenant='{}',id='{}',ns='{}'}}",
        u.tenant, u.id, u.ns);
  }
};

template <>
struct fmt::formatter<rgw_bucket> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const rgw_bucket& b, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    return fmt::format_to(ctx.out(), "rgw_bucket{{tenant='{}',name='{}',id='{}'}}",
        b.tenant, b.name, b.bucket_id);
  }
};

template <>
struct fmt::formatter<rgw::sal::User> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  // Annoyingly we can't use const rgw::sal::User& as some of the accessors
  // aren't marked as const.
  template <typename FormatContext>
  auto format(rgw::sal::User& u, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    // No point showing the id, it's just a composite of the fields we're
    // already showing.
    return fmt::format_to(ctx.out(), FMT_STRING("rgw::sal::User{{tenant='{}',name='{}',ns='{}'}}"),
        u.get_tenant(), u.get_display_name(), u.get_ns());
  }
};

template <>
struct fmt::formatter<RGWBucketInfo> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const RGWBucketInfo& info, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    const auto placement = info.placement_rule.empty() ? std::string { "<unset>" } : info.placement_rule.to_str();
    const auto sync_state = info.sync_policy ? "set" : "unset";

    return fmt::format_to(
        ctx.out(),
        FMT_STRING("RGWBucketInfo{{bucket={},owner={},zonegroup='{}',placement='{}',flags=0x{:x},versioned={},swift_versioning={},obj_lock_enabled={},requester_pays={},has_website={},mdsearch_fields={},sync_policy={}}}"),
        info.bucket,
        info.owner,
        info.zonegroup,
        placement,
        info.flags,
        info.versioned(),
        info.has_swift_versioning(),
        info.obj_lock_enabled(),
        info.requester_pays,
        info.has_website,
        info.mdsearch_config.size(),
        sync_state);
  }
};

template <>
struct fmt::formatter<rgw::sal::Attrs> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const rgw::sal::Attrs& attrs, FormatContext& ctx) const -> typename FormatContext::iterator
  {
    auto out = ctx.out();
    out = fmt::format_to(out, "rgw::sal::Attrs{{");

    bool first = true;
    for (const auto& [key, value] : attrs) {
      if (!first) {
        out = fmt::format_to(out, ", ");
      }
      first = false;

      // XXX TO BE CONTINUED: We should read the key name and decode the
      // values accordingly, for the keys we care about. For now, just output
      // the length.
      out = fmt::format_to(out, FMT_STRING("'{}':[{} bytes]"), key, value.length());
    }

    out = fmt::format_to(out, "}}");
    return out;
  }
};

/**
 * @brief Safely format a type at the end of a potentially-null pointer.
 *
 * Utility function for logging using fmtlib. Relies on type \p T having a
 * formatter.
 *
 * ```C++
 * // Safely format a potentially-null pointer to rgw_user.
 * rgw_user* u = ...;
 * ldpp_dout(dpp, 20) << fmt::format(FMT_STRING("User u={}"), fmt_maybe(u)) << dendl;
 * ```
 *
 * @tparam T
 * @param t
 * @return std::string
 */
template <typename T>
std::string fmt_maybe(T* t)
{
  if (t)
    return fmt::format(FMT_STRING("{}"), *t);
  else
    return "NULL";
}

/*****************************************************************************/

// Utility macros.

// Shorthand to check if we are configured at runtime to emit logs at a
// certain level. Used to avoid building log message strings unnecessarily.
#define LOG_ENABLED(cct, level) ((cct)->_conf->subsys.should_gather(dout_subsys, (level)))
// Variant taking a dpp instead of a cct.
#define LOG_ENABLED_PFX(dpp, level) LOG_ENABLED((dpp)->get_cct(), level)
// Global version.
#define LOG_ENABLED_G(level) LOG_ENABLED(g_ceph_context, level)

#define LOG(cct, level, msg, ...)                                                          \
  do {                                                                                     \
    ldout(cct, level) << fmt::format(FMT_STRING(msg) __VA_OPT__(, ) __VA_ARGS__) << dendl; \
  } while (0)

#define LOG_PFX(dpp, level, msg, ...)                                                          \
  do {                                                                                         \
    ldpp_dout(dpp, level) << fmt::format(FMT_STRING(msg) __VA_OPT__(, ) __VA_ARGS__) << dendl; \
  } while (0)

#define LOG_G(level, msg, ...) LOG(g_ceph_context, level, msg __VA_OPT__(, ) __VA_ARGS__)

#define COND_LOG(cct, level, msg, ...)                                                       \
  do {                                                                                       \
    if (LOG_ENABLED(cct, level)) {                                                           \
      ldout(cct, level) << fmt::format(FMT_STRING(msg) __VA_OPT__(, ) __VA_ARGS__) << dendl; \
    }                                                                                        \
  } while (0)
#define COND_LOG_PFX(dpp, level, msg, ...)                                                       \
  do {                                                                                           \
    if (LOG_ENABLED_PFX(dpp, level)) {                                                           \
      ldpp_dout(dpp, level) << fmt::format(FMT_STRING(msg) __VA_OPT__(, ) __VA_ARGS__) << dendl; \
    }                                                                                            \
  } while (0)
#define COND_LOG_G(level, msg, ...) COND_LOG(g_ceph_context, level, msg __VA_OPT__(, ) __VA_ARGS__)
