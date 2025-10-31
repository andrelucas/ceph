/**
 * @file test_mdoffload_util.h
 * @author André Lucas (alucas@akamai.com)
 * @brief Utilty code for test_rgw_mdoffload.
 * @version 0.1
 * @date 2025-10-31
 *
 * @copyright Copyright (c) 2025
 *
 */

#pragma once

#include <string>
#include <ostream>

#include <gtest/gtest.h>

#include "common/dout.h"

namespace akamai::test {

 /**
 * @brief Mixin to provide Ceph logging capabilities to gtest fixtures.
 *
 * Add this to the bases for a gtest fixture class, and the fixture class
 * becomes a DoutPrefixProvider that logs with the test suite and test name,
 * as well as a line number. You can then use `ldpp_dout(this, level)` in your
 * tests.
 *
 * ``` c++
 * class MyTestSuite : public ::testing::Test, public CephGtestLogAdapter {
 *   // ...
 *  };
 * ```
 *
 * Suite names and test names are truncated to a maximum width \p W, with
 * leading ellipses if truncation occurs. Type or value parameters are also
 * printed if in effect. The line number is helpful to identify the exact log
 * location, especially if names get truncated.
 */
class CephGtestLogAdapter : public DoutPrefixProvider {
private:
  CephContext* cct_;
  unsigned subsys_;
  std::string prefix_;
  const ::testing::TestInfo* ti_;

public:
  /// The maximum width for the test suite and test names.
  static constexpr std::size_t W = 80;

public:
  CephGtestLogAdapter()
      : cct_ { g_ceph_context }
      , subsys_ { ceph_subsys_rgw }
  {
    ti_ = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string suite = ellipsize(ti_->test_suite_name());
    std::string test = ellipsize(ti_->name());
    std::stringstream ss;
    // There's no interface for both type_param and value_param being set, so
    // we can just switch.
    if (ti_->type_param()) {
      ss << fmt::format(FMT_STRING("{}.{}/{}: "), suite, test, ti_->type_param());
    } else if (ti_->value_param()) {
      ss << fmt::format(FMT_STRING("{}.{}/{}: "), suite, test, ti_->value_param());
    } else {
      ss << fmt::format(FMT_STRING("{}.{}: "), suite, test);
    }
    ss << fmt::format(FMT_STRING("line {}: "), ti_->line());
    prefix_ = ss.str();
  }

  static std::string ellipsize(const std::string& str)
  {
    if (str.size() <= W) {
      return str;
    } else {
      // Use Unicode ellipsis character instead of three dots
      return "…" + str.substr(str.size() - (W - 1), W - 1);
    }
  }

  std::ostream& gen_prefix(std::ostream& out) const override
  {
    return out << prefix_;
  }

  CephContext* get_cct() const override
  {
    return cct_;
  }

  unsigned get_subsys() const override
  {
    return subsys_;
  }

}; // class CephLogAdapter

inline std::ostream& operator<<(
    std::ostream& lhs, const CephGtestLogAdapter& a)
{
  return a.gen_prefix(lhs);
}
#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<CephGtestLogAdapter> : fmt::ostream_formatter {
};
#endif

} // namespace akamai::test
