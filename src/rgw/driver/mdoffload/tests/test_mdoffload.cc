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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/async/yield_context.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/dout.h"
#include "common/subsys_types.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_sal_mdoffload.h"

#include "mock_sal.h"

namespace {

using MDOffloadFilterDriver = rgw::sal::MDOffloadFilterDriver;

// Create a filter driver.
TEST(RGWMDOffloadFilterDriver, CreateNullptrNextThrows)
{
  ASSERT_THROW(newMDOffloadFilter(g_ceph_context, nullptr), MDOffloadFilterDriver::BadNextDriver);
}

/**
 * @brief Mock fixture to test MDOffloadFilterDriver via our mocked base
 * driver.
 *
 * Where possible we'll just use the mock (empty) implementations. However,
 * because of the way rgw::sal::Driver and its subsidiary classes work, there
 * are methods where we'll need to implement something (probably via WillOnce
 * or WithArg<>) that tweaks return values into the proper types.
 */
class RGWMDOffloadFilterDriverMockFixture : public ::testing::Test {
protected:
  DoutPrefix dpp_ { g_ceph_context, ceph_subsys_test, "test_mdoffload" };
  DoutPrefixProvider* dpp;

  ::testing::NiceMock<akamai::mock::MockDriver> mock_base;
  std::unique_ptr<rgw::sal::Driver> filter;

public:
  void SetUp() override
  {
    dpp = &dpp_; // It's just easier to have 'dpp'.
    auto ret = mock_base.initialize(g_ceph_context, &dpp_);
    ASSERT_GE(ret, 0);
  }
  void TearDown() override
  {
    filter->finalize();
  }

  // Helper to get a MDOffloadBucket via the filter. The underlying Bucket (in
  // this->next) will be a MockBucket.
  std::unique_ptr<rgw::sal::MDOffloadBucket> get_bucket()
  {
    rgw_user u;
    u.id = "test_user";
    std::unique_ptr<rgw::sal::User> user;
    rgw_bucket b;
    b.name = "test_bucket";
    optional_yield y = null_yield;
    std::unique_ptr<rgw::sal::Bucket> bucket;

    using namespace ::testing;

    EXPECT_CALL(mock_base, get_user(u)) //
        .Times(1);

    EXPECT_CALL(mock_base, get_bucket(_, _, _, _, _)) //
        .Times(1)
        .WillOnce(
            DoAll(
                WithArg<3>([](std::unique_ptr<rgw::sal::Bucket>* out) {
                  out->reset(new akamai::mock::MockBucket());
                }),
                Return(0)));

    user = filter->get_user(u);
    filter->get_bucket(dpp, user.get(), b, &bucket, y);
    return std::unique_ptr<rgw::sal::MDOffloadBucket>(
        dynamic_cast<rgw::sal::MDOffloadBucket*>(bucket.release()));
  }
};

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMockCreateValidNextSucceeds)
{

  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });

  ASSERT_NE(filter, nullptr);

  EXPECT_CALL(mock_base, get_name()) //
      .Times(1);

  // The filter's get_name() will call the mock's get_name(), which will
  // return an empty string.
  ASSERT_EQ(filter->get_name(), "mdoffload<>");
}

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMockGetBucket)
{

  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });

  ASSERT_NE(filter, nullptr);

  rgw_user u;
  u.id = "test_user";

  std::unique_ptr<rgw::sal::User> user;

  rgw_bucket b;
  b.name = "test_bucket";
  optional_yield y = null_yield;

  std::unique_ptr<rgw::sal::Bucket> bucket;

  EXPECT_CALL(mock_base, get_user(u)) //
      .Times(1);
  EXPECT_CALL(mock_base, get_bucket(dpp, testing::_, b, testing::_, y)) //
      .Times(1);

  user = filter->get_user(u);
  ASSERT_NE(user, nullptr);
  filter->get_bucket(dpp, user.get(), b, &bucket, y);
  ASSERT_NE(bucket, nullptr);

  filter->finalize();
}

// get_attr() MUST NOT call next->get_attr(). It must handle it itself.
TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Bucket_get_attr_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });

  using ::testing::DefaultValue;

  // Set up a default return value for Attrs&.
  rgw::sal::Attrs empty_attrs;
  DefaultValue<::rgw::sal::Attrs&>::Set(empty_attrs);

  ASSERT_NE(filter, nullptr);
  auto bucket = get_bucket();
  ASSERT_NE(bucket, nullptr);
  auto mock_bucket = dynamic_cast<akamai::mock::MockBucket*>(bucket->get_next());
  ASSERT_NE(mock_bucket, nullptr);
  EXPECT_CALL(*mock_bucket, get_attrs()) //
      .Times(0);
  bucket->get_attrs();

  filter->finalize();
}

// merge_and_store_attrs() MUST NOT call next->merge_and_store_attrs(). It must handle it itself.
TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Bucket_merge_and_store_attrs_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });

  using ::testing::_;
  using ::testing::DefaultValue;

  // Set up a default return value for Attrs&.
  rgw::sal::Attrs empty_attrs;
  DefaultValue<::rgw::sal::Attrs&>::Set(empty_attrs);

  ASSERT_NE(filter, nullptr);
  auto bucket = get_bucket();
  ASSERT_NE(bucket, nullptr);
  auto mock_bucket = dynamic_cast<akamai::mock::MockBucket*>(bucket->get_next());
  ASSERT_NE(mock_bucket, nullptr);
  EXPECT_CALL(*mock_bucket, merge_and_store_attrs) //
      .Times(0);
  bucket->merge_and_store_attrs(dpp, empty_attrs, null_yield);

  filter->finalize();
}

} // empty namespace

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  // Let the caller change the library debug level.
  if (std::getenv("TEST_DEBUG")) {
    std::string err;
    int level = strict_strtol(std::getenv("TEST_DEBUG"), 10, &err);
    if (err.empty()) {
      g_ceph_context->_conf->subsys.set_log_level(ceph_subsys_rgw, std::min(level, 30));
    }
  }

  common_init_finish(g_ceph_context);
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
