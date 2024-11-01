/**
 * @file test_rgw_cxa_throw_wrapper.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Unit test for __cxa_throw wrapper (in librgw_common.a)
 * @version 0.1
 * @date 2024-11-01
 *
 * @copyright Copyright (c) 2024
 *
 */

#include "rgw_cxa_throw_wrapper.h"
#include <gtest/gtest-death-test.h>
#include <gtest/gtest.h>

namespace {

constexpr uint64_t rep_count = 10000;

// Check that normal exceptions work normally and that we appear to visit the
// wrapper.
TEST(TestRgwCxaThrowWrapper, NormalThrow)
{
  for (uint64_t n = 0; n < rep_count; n++) {
    uint64_t exception_count = _cxa_throw_exception_count;
    try {
      throw std::runtime_error("Normal throw");
    } catch (const std::exception& e) {
      //   std::cerr << "Caught exception: " << e.what() << std::endl;
    }
    ASSERT_GT(_cxa_throw_exception_count, exception_count);
  }
}

// Check that nested exceptions work normally and that we appear to visit the
// wrapper.
TEST(TestRgwCxaThrowWrapper, NestedThrow)
{
  for (uint64_t n = 0; n < rep_count; n++) {
    uint64_t exception_count = _cxa_throw_exception_count;
    try {
      try {
        throw std::runtime_error("Nested throw");
      } catch (const std::exception& e) {
        // std::cerr << "Caught exception: " << e.what() << std::endl;
        throw;
      }
    } catch (const std::exception& e) {
      // std::cerr << "Caught exception: " << e.what() << std::endl;
    }
    ASSERT_GT(_cxa_throw_exception_count, exception_count);
  }
}

/**
 * Replicate Vitaly's standalone cxa_throw test inside of gtest.
 *
 * The test program sets up to throw an exception during stack unwinding.
 * Under normal circumstances this will cause the program to terminate with
 * SIGABRT. The test program catches SIGABRT; we're going to do it the gtest
 * way.
 *
 * Make a death test that will assert that the program is terminated by the
 * multiple throws of an exception.
 *
 * The real point, though, is to allow us to set a breakpoint in the wrapper
 * version of __cxa_throw. We can run the test, set the breakpoint, and check
 * that we've got all the linkage correct.
 *
 */

class Cleanup {
public:
  ~Cleanup() noexcept(false)
  {
    std::cout << "Cleanup function called during stack unwinding" << std::endl;
    throw std::runtime_error("Error in cleanup function"); // Throwing during stack unwinding
  }
};

void recursiveFunction(int depth)
{
  if (std::uncaught_exceptions() > 0) {
    std::cerr << "Zhopa: " << std::endl;
    return;
  }
  try {
    Cleanup cleanup; // This object will be destroyed during stack unwinding
    if (depth == 3) {
      throw std::runtime_error("Initial exception");
    }
    recursiveFunction(depth + 1);
  } catch (const std::exception& e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
  }
}

TEST(TestRgwCxaThrowWrapper, RecursiveThrowDeathTest)
{
  ::testing::GTEST_FLAG(death_test_style) = "threadsafe";
  ASSERT_DEATH(recursiveFunction(0), "Error in cleanup function");
}

}
