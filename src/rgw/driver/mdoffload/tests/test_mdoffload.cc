/**
 * @file test_mdoffload.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief
 * @version 0.1
 * @date 2025-09-23
 *
 * @copyright Copyright (c) 2025
 *
 */

#include "common/dout.h"
#include "common/subsys_types.h"
#include "global/global_context.h"
#include "rgw_sal_mdoffload.h"

#include "mock_sal.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using MDOffloadFilterDriver = rgw::sal::MDOffloadFilterDriver;

// Create a filter driver.
TEST(RGWMDOffloadFilterDriver, CreateNullptrNextThrows)
{
  ASSERT_THROW(newMDOffloadFilter(g_ceph_context, nullptr), MDOffloadFilterDriver::BadNextDriver);
}

class RGWMDOffloadFilterDriverFixture : public ::testing::Test {
protected:
  DoutPrefix dpp_ { g_ceph_context, ceph_subsys_test, "test_mdoffload" };
};

TEST_F(RGWMDOffloadFilterDriverFixture, WithMockCreateValidNextSucceeds)
{
  ::testing::NiceMock<rgw::sal::MockDriver> next;
  auto ret = next.initialize(g_ceph_context, &dpp_);
  ASSERT_GE(ret, 0);

  std::unique_ptr<rgw::sal::Driver> filter;
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &next)); });

  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(filter->get_name(), "mdoffload<mockdriver>");
}

} // empty namespace
