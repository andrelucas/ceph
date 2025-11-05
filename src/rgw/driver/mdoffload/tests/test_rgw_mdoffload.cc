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
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "common/async/yield_context.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/subsys_types.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/buffer_fwd.h"
#include "mdoffload/v1/mdoffload.pb.h"
#include "rgw_common.h"
#include "rgw_placement_types.h"
#include "rgw_sal.h"
#include "rgw_sal_mdoffload.h"

#include "mock_sal.h"
#include "test_rgw_grpc_util.h"
#include "test_rgw_mdoffload_grpcutil.h"
#include "test_rgw_mdoffload_util.h"

namespace {

using MDOffloadFilterDriver = rgw::sal::MDOffloadFilterDriver;

/* #region Mock */

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
 * are methods where we'll need to implement something (via WithArg<> and
 * Return) that tweaks return values into the proper types.
 */
class RGWMDOffloadFilterDriverMockFixture : public ::testing::Test, public akamai::test::CephGtestLogAdapter {

protected:
  ::testing::NiceMock<akamai::mock::MockDriver> mock_base;
  std::unique_ptr<rgw::sal::Driver> filter;

public:
  void SetUp() override
  {
    // ldpp_dout(this, 10) << "RGWMDOffloadFilterDriverMockFixture::SetUp" << dendl;
    auto ret = mock_base.initialize(g_ceph_context, this);
    ASSERT_GE(ret, 0);
  }
  void TearDown() override
  {
    if (filter) {
      filter->finalize();
      filter.reset();
    }
  }

  /**
   * @brief Get a bucket object from the filter, using the mock base.
   *
   * Helper to get a MDOffloadBucket via the filter. The underlying Bucket (in
   * this->next) will be a MockBucket, so we have to do some extra work with
   * the MockDriver.
   *
   * @return std::unique_ptr<rgw::sal::MDOffloadBucket>
   */
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

    // We don't care about the user object here, so it can be a default from
    // the mock.
    EXPECT_CALL(mock_base, get_user(u)) //
        .Times(1);

    // We need mock_base.get_bucket() to return a valid MockBucket, as we'll
    // be using its mock later.
    EXPECT_CALL(mock_base, get_bucket(_, _, _, _, _)) //
        .Times(1)
        .WillOnce(
            DoAll(
                WithArg<3>([](std::unique_ptr<rgw::sal::Bucket>* out) {
                  out->reset(new akamai::mock::MockBucket());
                }),
                Return(0)));

    user = filter->get_user(u);
    filter->get_bucket(this, user.get(), b, &bucket, y);

    Mock::VerifyAndClearExpectations(&mock_base);

    return std::unique_ptr<rgw::sal::MDOffloadBucket>(
        dynamic_cast<rgw::sal::MDOffloadBucket*>(bucket.release()));
  }

  /**
   * @brief Get a user object from the filter, using the mock base.
   *
   * Helper to get a MDOffloadUser via the filter. The underlying User (in
   * this->next) will be a MockUser, so we have to do some extra work with
   * the MockDriver.
   *
   * @return std::unique_ptr<rgw::sal::MDOffloadUser>
   */
  std::unique_ptr<rgw::sal::MDOffloadUser> get_user()
  {
    rgw_user u;
    u.id = "test_user";
    std::unique_ptr<rgw::sal::User> sal_user;

    using namespace ::testing;

    // We need <MockDriver>::get_user() to return a valid MockUser, as we'll
    // be using its mock later.
    EXPECT_CALL(mock_base, get_user(u)) //
        .Times(1)
        .WillOnce(
            Return(ByMove(std::unique_ptr<rgw::sal::User>(new akamai::mock::MockUser()))));

    sal_user = filter->get_user(u);

    Mock::VerifyAndClearExpectations(&mock_base);

    return std::unique_ptr<rgw::sal::MDOffloadUser>(
        dynamic_cast<rgw::sal::MDOffloadUser*>(sal_user.release()));
  }

  std::unique_ptr<rgw::sal::MDOffloadObject> get_object()
  {
    auto bucket = get_bucket();
    auto mock_bucket = dynamic_cast<akamai::mock::MockBucket*>(bucket->get_next());

    using namespace ::testing;

    EXPECT_CALL(*mock_bucket, get_object)
        .Times(1)
        .WillOnce(
            DoAll(
                Return(ByMove(std::unique_ptr<rgw::sal::Object>(new akamai::mock::MockObject())))));

    rgw_obj_key obj_key("test_object_key");
    auto object = bucket->get_object(obj_key);

    Mock::VerifyAndClearExpectations(&mock_bucket);

    return std::unique_ptr<rgw::sal::MDOffloadObject>(
        dynamic_cast<rgw::sal::MDOffloadObject*>(object.release()));
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
  EXPECT_CALL(mock_base, get_bucket(this, testing::_, b, testing::_, y)) //
      .Times(1);

  user = filter->get_user(u);
  ASSERT_NE(user, nullptr);
  filter->get_bucket(this, user.get(), b, &bucket, y);
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

// set_attr() MUST NOT call next->set_attr(). It must handle it itself.
TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Bucket_set_attr_MustNotCallParent)
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
  EXPECT_CALL(*mock_bucket, set_attrs(testing::_)) //
      .Times(0);
  bucket->set_attrs(empty_attrs);

  filter->finalize();
}

// merge_and_store_attrs() MUST NOT call next->merge_and_store_attrs(). It must handle it itself.
TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Bucket_merge_and_store_attrs_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  ASSERT_NE(filter, nullptr);

  using ::testing::_;
  using ::testing::DefaultValue;

  // Set up a default return value for Attrs&.
  rgw::sal::Attrs empty_attrs;
  DefaultValue<::rgw::sal::Attrs&>::Set(empty_attrs);

  auto bucket = get_bucket();
  ASSERT_NE(bucket, nullptr);
  auto mock_bucket = dynamic_cast<akamai::mock::MockBucket*>(bucket->get_next());
  ASSERT_NE(mock_bucket, nullptr);
  EXPECT_CALL(*mock_bucket, merge_and_store_attrs) //
      .Times(0);
  bucket->merge_and_store_attrs(this, empty_attrs, null_yield);

  filter->finalize();
}

// Unfortunately we can't rely on <Bucket>->set_attr() interception, as
// set_attr() is never called (in v18.2.7). Instead, merge_and_store_attrs()
// is always used except for initial instantiation at
// <Driver>->create_bucket() time. We have to satisfy ourselves that we're
// intercepting create_bucket() properly and passing empty attrs to the
// parent.
TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Bucket_create_bucket_MustSendEmptyAttrsToParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  auto check_driver = dynamic_cast<MDOffloadFilterDriver*>(filter.get());
  ASSERT_NE(check_driver, nullptr);

  // Fetch a user with a 'real' MockUser as its ->next;
  auto offload_user = get_user();
  ASSERT_NE(offload_user, nullptr);
  auto mock_user = dynamic_cast<akamai::mock::MockUser*>(offload_user->get_next());
  ASSERT_NE(mock_user, nullptr);

  using namespace ::testing;

  // This expectation tests that the attrs passed to the parent MockUser
  // object are empty, despite the fact that we explicitly set the attributes
  // to the MDOffloadFilterUser::create_bucket() call to be nonempty. This
  // implies strongly (but doesn't prove) that we are intercepting
  // create_bucket() properly.
  //
  // This is made more confusing by the fact that create_bucket() takes so
  // many parameters (16) that our mock is indirected via create_bucket_cb(),
  // taking a single struct parameter. The testing::A<> matcher is magic that
  // Copilot came up with, it allows for better type checking.
  //
  EXPECT_CALL(*mock_user, create_bucket_cb(testing::A<const akamai::mock::CreateBucketParams&>()))
      .Times(1)
      .WillOnce(
          testing::DoAll(
              testing::WithArg<0>([](const akamai::mock::CreateBucketParams& p) {
                // The attrs passed to the parent must be empty.
                ASSERT_NE(p.attrs, nullptr);
                EXPECT_TRUE(p.attrs->empty());
              }),
              testing::Return(0)));

  // A long and tiresome list of parameters to create_bucket().
  rgw_bucket b;
  b.name = "test_bucket";
  rgw_placement_rule placement {};
  std::string swift_ver_location {};
  rgw::sal::Attrs nonempty_attrs;
  nonempty_attrs["key1"] = ceph::bufferlist();
  RGWBucketInfo binfo;
  obj_version objv;
  RGWEnv env;
  req_info req(g_ceph_context, &env);
  // This receives the created bucket.
  std::unique_ptr<rgw::sal::Bucket> bucket_out;

  // We're passing in nonempty attrs to the filter create_bucket().
  ASSERT_FALSE(nonempty_attrs.empty());

  offload_user->create_bucket(this,
      b,
      "",
      placement,
      swift_ver_location,
      nullptr,
      RGWAccessControlPolicy {},
      nonempty_attrs,
      binfo,
      objv,
      false,
      false,
      nullptr,
      req,
      &bucket_out,
      null_yield);

  ASSERT_NE(bucket_out, nullptr);

  filter->finalize();
}

// Manually exercise the <Bucket>->get_object() path.
TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Bucket_get_object_PathManual)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  auto check_driver = dynamic_cast<MDOffloadFilterDriver*>(filter.get());
  ASSERT_NE(check_driver, nullptr);

  // The useful get_object() is a call on rgw::sal::Bucket. The get_object()
  // on rgw::sal::Driver doesn't actually fetch anything.
  auto bucket = get_bucket();
  ASSERT_NE(bucket, nullptr);
  auto mock_bucket = dynamic_cast<akamai::mock::MockBucket*>(bucket->get_next());
  ASSERT_NE(mock_bucket, nullptr);

  using namespace ::testing;

  EXPECT_CALL(*mock_bucket, get_object)
      .Times(1)
      .WillOnce(
          DoAll(
              Return(ByMove(std::unique_ptr<rgw::sal::Object>(new akamai::mock::MockObject())))));

  rgw_obj_key obj_key("test_object_key");
  auto object = bucket->get_object(obj_key);
  ASSERT_NE(object, nullptr);
  auto mdo_object = dynamic_cast<rgw::sal::MDOffloadObject*>(object.release());
  ASSERT_NE(mdo_object, nullptr);
  auto mock_object = dynamic_cast<akamai::mock::MockObject*>(mdo_object->get_next());
  ASSERT_NE(mock_object, nullptr);
}

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Bucket_get_object_Method)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  auto check_driver = dynamic_cast<MDOffloadFilterDriver*>(filter.get());
  ASSERT_NE(check_driver, nullptr);

  auto object = get_object();
  ASSERT_NE(object, nullptr);
  auto mock_object = dynamic_cast<akamai::mock::MockObject*>(object->get_next());
  ASSERT_NE(mock_object, nullptr);
}

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Object_set_obj_attrs_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  ASSERT_NE(filter, nullptr);

  using ::testing::DefaultValue;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  auto object = get_object();
  ASSERT_NE(object, nullptr);
  auto mock_object = dynamic_cast<akamai::mock::MockObject*>(object->get_next());
  ASSERT_NE(mock_object, nullptr);

  // The mock object's set_obj_attrs() MUST NOT be called.
  EXPECT_CALL(*mock_object, set_obj_attrs(testing::_, testing::_, testing::_, testing::_)) //
      .Times(0);

  rgw::sal::Attrs setattrs;
  rgw::sal::Attrs delattrs;
  auto ret = object->set_obj_attrs(this, &setattrs, &delattrs, null_yield);
  ASSERT_GE(ret, 0);

  // Bonus test: Need to set has_attrs_.
  ASSERT_TRUE(object->has_attrs());

  filter->finalize();
}

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Object_get_obj_attrs_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  ASSERT_NE(filter, nullptr);

  using ::testing::DefaultValue;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  auto object = get_object();
  ASSERT_NE(object, nullptr);
  auto mock_object = dynamic_cast<akamai::mock::MockObject*>(object->get_next());
  ASSERT_NE(mock_object, nullptr);

  // The mock object's get_obj_attrs() MUST NOT be called.
  EXPECT_CALL(*mock_object, get_obj_attrs) //
      .Times(0);

  auto ret = object->get_obj_attrs(null_yield, this, nullptr);
  ASSERT_GE(ret, 0);

  filter->finalize();
}

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Object_modify_obj_attrs_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  ASSERT_NE(filter, nullptr);

  using ::testing::DefaultValue;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  auto object = get_object();
  ASSERT_NE(object, nullptr);
  auto mock_object = dynamic_cast<akamai::mock::MockObject*>(object->get_next());
  ASSERT_NE(mock_object, nullptr);

  // The mock object's modify_obj_attrs() MUST NOT be called.
  EXPECT_CALL(*mock_object, modify_obj_attrs) //
      .Times(0);

  ceph::bufferlist attr_val;
  auto ret = object->modify_obj_attrs("test_attr", attr_val, null_yield, this);
  ASSERT_GE(ret, 0);

  filter->finalize();
}

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Object_delete_obj_attrs_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  ASSERT_NE(filter, nullptr);

  using ::testing::DefaultValue;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  auto object = get_object();
  ASSERT_NE(object, nullptr);
  auto mock_object = dynamic_cast<akamai::mock::MockObject*>(object->get_next());
  ASSERT_NE(mock_object, nullptr);

  // The mock object's delete_obj_attrs() MUST NOT be called.
  EXPECT_CALL(*mock_object, delete_obj_attrs) //
      .Times(0);

  auto ret = object->delete_obj_attrs(this, "test_attr", null_yield);
  ASSERT_GE(ret, 0);

  filter->finalize();
}

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Object_get_attrs_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  ASSERT_NE(filter, nullptr);

  using ::testing::DefaultValue;

  // Set up a default return value for rgw::sal::Attrs&.
  rgw::sal::Attrs empty_attrs;
  DefaultValue<rgw::sal::Attrs&>::Set(empty_attrs);

  auto object = get_object();
  ASSERT_NE(object, nullptr);
  auto mock_object = dynamic_cast<akamai::mock::MockObject*>(object->get_next());
  ASSERT_NE(mock_object, nullptr);

  EXPECT_CALL(*mock_object, get_attrs()) //
      .Times(0);

  object->get_attrs();

  filter->finalize();
}

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Object_set_attrs_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  ASSERT_NE(filter, nullptr);

  using ::testing::DefaultValue;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  auto object = get_object();
  ASSERT_NE(object, nullptr);
  auto mock_object = dynamic_cast<akamai::mock::MockObject*>(object->get_next());
  ASSERT_NE(mock_object, nullptr);

  EXPECT_CALL(*mock_object, set_attrs(testing::_)) //
      .Times(0);

  rgw::sal::Attrs attrs;
  auto ret = object->set_attrs(attrs);
  ASSERT_GE(ret, 0);

  filter->finalize();
}

TEST_F(RGWMDOffloadFilterDriverMockFixture, WithMock_Object_has_attrs_MustNotCallParent)
{
  EXPECT_NO_THROW({ filter.reset(newMDOffloadFilter(g_ceph_context, &mock_base)); });
  ASSERT_NE(filter, nullptr);

  using ::testing::DefaultValue;

  // Set up a default return value for bool.
  DefaultValue<bool>::Set(false);

  auto object = get_object();
  ASSERT_NE(object, nullptr);
  auto mock_object = dynamic_cast<akamai::mock::MockObject*>(object->get_next());
  ASSERT_NE(mock_object, nullptr);

  EXPECT_CALL(*mock_object, has_attrs()) //
      .Times(0);

  object->has_attrs();

  filter->finalize();
}

/* #endregion Mock */
/****************************************************************************/

/* #region Grpc */

class MDOffloadGrpcTestServer : public ::testing::Test, public akamai::test::CephGtestLogAdapter {
public:
  // Note there are more tests for the test server MDOffloadServiceImpl in
  // test_rgw_mdoffload_grpcservice.cc.
  using server_type = GRPCTestServer<akamai::test::MDOffloadServiceImpl>;

protected:
  server_type server_;

  void TearDown() override { server_.stop(); }
  server_type& server() { return server_; }
}; // class MDOffloadGrpcMock

// Make sure the server objects are properly created and destroyed.
TEST_F(MDOffloadGrpcTestServer, Null)
{
}

TEST_F(MDOffloadGrpcTestServer, MetaStart)
{
  server().start();
  for (int n = 0; n < 1000; n++) {
    server().start();
  }
  server().stop();
}

TEST_F(MDOffloadGrpcTestServer, MetaStop)
{
  server().start();
  for (int n = 0; n < 1000; n++) {
    server().stop();
  }
}

/* #endregion Grpc */

/****************************************************************************/

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

  g_ceph_context->_conf->log_flush_on_exit = true;
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleMock(&argc, argv);
  int ret = RUN_ALL_TESTS();

  return ret;
}
