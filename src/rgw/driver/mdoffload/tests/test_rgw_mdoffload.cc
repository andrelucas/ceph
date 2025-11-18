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
#include "rgw_common.h"
#include "rgw_placement_types.h"
#include "rgw_sal.h"
#include "rgw_sal_mdoffload.h"

#include "test_rgw_grpc_util.h"
#include "test_rgw_mdoffload_grpcutil.h"
#include "test_rgw_mdoffload_util.h"
#include "tests/fake_sal.h"

namespace {

using MDOffloadFilterDriver = rgw::sal::MDOffloadFilterDriver;

// Create a filter driver.
TEST(RGWMDOffloadFilterDriver, CreateNullptrNextThrows)
{
  ASSERT_THROW(newMDOffloadFilter(g_ceph_context, nullptr), MDOffloadFilterDriver::BadNextDriver);
}

/****************************************************************************/

/* #region Fake */

class RGWMDOffloadFakeDriverFixture : public ::testing::Test, public akamai::test::CephGtestLogAdapter {
public:
  // Note there are more tests for the test server MDOffloadServiceImpl in
  // test_rgw_mdoffload_grpcservice.cc.
  using server_type = GRPCTestServer<akamai::test::MDOffloadServiceImpl>;

protected:
  server_type server_;
  std::unique_ptr<akamai::fake::FakeDriver> fake_base_;
  std::unique_ptr<rgw::sal::MDOffloadFilterDriver> filter_;

  void SetUp()
  {
    // Start a gRPC server that automatically instantiates buckets and objects
    // as needed. This allows us to test the filter driver on its fake base
    // without needing all sorts of test-only special cases in the driver
    // itself.
    server_.start();
    auto uri = server_.address();
    server_.instance()->set_create_if_missing(true);
    // Point the filter driver at our test server.
    g_ceph_context->_conf->rgw_mdoffload_grpc_uri = uri;

    // The base (next) driver must be initialized before the filter.
    fake_base_.reset(new akamai::fake::FakeDriver());
    auto ret = fake_base_->initialize(g_ceph_context, this);
    ASSERT_GE(ret, 0);
    // Initialise the filter driver. This will set up the gRPC client channel
    // wrapper, among other things.
    auto driver = dynamic_cast<rgw::sal::MDOffloadFilterDriver*>(newMDOffloadFilter(g_ceph_context, fake_base_.get()));
    ret = driver->initialize(g_ceph_context, this);
    ASSERT_GE(ret, 0);
    filter_.reset(driver);
    ASSERT_NE(filter_, nullptr);
  }

  void TearDown()
  {
    if (filter_) {
      filter_->finalize();
      filter_.reset();
    }
    if (fake_base_) {
      fake_base_->finalize();
      fake_base_.reset();
    }
    server_.stop();
  }

  // Fetch a bucket via the filter. The returned bucket is guaranteed to be a
  // MDOffloadBucket.
  void fixture_get_bucket(const std::string& bucket_name,
      std::unique_ptr<rgw::sal::MDOffloadBucket>* filter_bucket_out)
  {
    ASSERT_NE(filter_bucket_out, nullptr) << "Must provide output parameter";
    rgw_user u;
    u.id = "test_user";
    rgw_bucket b;
    b.name = bucket_name;
    b.bucket_id = akamai::fake::stable_uuid_for_bucket_name(b.name);
    optional_yield y = null_yield;
    std::unique_ptr<rgw::sal::Bucket> bucket;

    auto user = filter_->get_user(u);
    ASSERT_TRUE(user);

    auto ret = filter_->get_bucket(this, user.get(), b, &bucket, y);
    ASSERT_GE(ret, 0);
    ASSERT_NE(bucket, nullptr);
    EXPECT_EQ(bucket->get_name(), b.name);
    EXPECT_EQ(bucket->get_bucket_id(), b.bucket_id);

    filter_bucket_out->reset(dynamic_cast<rgw::sal::MDOffloadBucket*>(bucket.release()));
    ASSERT_TRUE(*filter_bucket_out);
  }

  // Get object with an existing bucket. The returned object is guaranteed to be
  // a MDOffloadObject.
  void fixture_get_object(rgw::sal::Bucket* bucket, rgw_obj_key key, std::unique_ptr<rgw::sal::MDOffloadObject>* filter_object_out)
  {
    ASSERT_NE(bucket, nullptr) << "Must provide valid bucket";
    ASSERT_NE(filter_object_out, nullptr) << "Must provide output parameter";

    auto object = bucket->get_object(key);
    ASSERT_NE(object, nullptr);
    auto mdo_object = dynamic_cast<rgw::sal::MDOffloadObject*>(object.release());
    ASSERT_NE(mdo_object, nullptr);

    filter_object_out->reset(mdo_object);
    ASSERT_TRUE(*filter_object_out);
  }

  // One-shot get object via bucket name and object key. The returned object is
  // guaranteed to be a MDOffloadObject.
  void fixture_get_object(const std::string& bucket_name, rgw_obj_key key, std::unique_ptr<rgw::sal::MDOffloadObject>* filter_object_out)
  {
    std::unique_ptr<rgw::sal::MDOffloadBucket> filter_bucket;
    fixture_get_bucket(bucket_name, &filter_bucket);
    fixture_get_object(filter_bucket.get(), key, filter_object_out);
  }

}; // class RGWMDOffloadFakeDriverFixture

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_CreateValidNextSucceeds)
{
  auto name = filter_->get_name();
  // The filter's get_name() will call the fake's get_name(), which will
  // return "fake-sal".
  ASSERT_EQ(name, "mdoffload<fake-sal>");
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_GetUserSucceeds)
{
  rgw_user u;
  u.id = "test_user";
  u.tenant = "test_tenant";
  u.ns = "test_ns";

  // This depends on FakeDriver::get_user() copying the rgw_user fields into
  // the FakeUser, and in particular using the id as the display_name since
  // there's no backend from which to fetch a proper display_name.
  auto user = filter_->get_user(u);
  ASSERT_NE(user, nullptr);
  EXPECT_EQ(user->get_display_name(), u.id);
  EXPECT_EQ(user->get_tenant(), u.tenant);
  EXPECT_EQ(user->get_ns(), u.ns);
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_GetBucketVariant1Succeeds)
{
  rgw_user u;
  u.id = "test_user";
  rgw_bucket b;
  b.name = "test_bucket";
  b.bucket_id = akamai::fake::stable_uuid_for_bucket_name(b.name);
  optional_yield y = null_yield;
  std::unique_ptr<rgw::sal::Bucket> bucket;

  auto user = filter_->get_user(u);
  ASSERT_NE(user, nullptr);

  auto ret = filter_->get_bucket(this, user.get(), b, &bucket, y);
  ASSERT_GE(ret, 0);
  ASSERT_NE(bucket, nullptr);
  EXPECT_EQ(bucket->get_name(), b.name);
  EXPECT_EQ(bucket->get_bucket_id(), b.bucket_id);

  // Check the type of the returned bucket.
  auto mdo_bucket = dynamic_cast<rgw::sal::MDOffloadBucket*>(bucket.get());
  ASSERT_NE(mdo_bucket, nullptr);
  // Check the type of the underlying bucket.
  auto fake_bucket = dynamic_cast<akamai::fake::FakeBucket*>(mdo_bucket->get_next());
  ASSERT_NE(fake_bucket, nullptr);
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_GetBucketVariant3Succeeds)
{
  rgw_user u;
  u.id = "test_user";
  std::string bucket_name = "test_bucket";
  optional_yield y = null_yield;
  std::unique_ptr<rgw::sal::Bucket> bucket;

  auto user = filter_->get_user(u);
  ASSERT_NE(user, nullptr);

  // Variant 3 is called by create_bucket().
  auto ret = filter_->get_bucket(this, user.get(), "", bucket_name, &bucket, y);
  ASSERT_GE(ret, 0);
  ASSERT_NE(bucket, nullptr);
  EXPECT_EQ(bucket->get_name(), bucket_name);
  // No bucket_id is provided in this variant.
  EXPECT_EQ(bucket->get_bucket_id(), "");

  // Check the type of the returned bucket.
  auto mdo_bucket = dynamic_cast<rgw::sal::MDOffloadBucket*>(bucket.get());
  ASSERT_NE(mdo_bucket, nullptr);
  // Check the type of the underlying bucket.
  auto fake_bucket = dynamic_cast<akamai::fake::FakeBucket*>(mdo_bucket->get_next());
  ASSERT_NE(fake_bucket, nullptr);
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_GetObjectSucceeds)
{
  rgw_user u;
  u.id = "test_user";
  rgw_bucket b;
  b.name = "test_bucket";
  b.bucket_id = akamai::fake::stable_uuid_for_bucket_name(b.name);
  optional_yield y = null_yield;
  std::unique_ptr<rgw::sal::Bucket> bucket;

  auto user = filter_->get_user(u);
  ASSERT_NE(user, nullptr);

  auto ret = filter_->get_bucket(this, user.get(), b, &bucket, y);
  ASSERT_GE(ret, 0);
  ASSERT_NE(bucket, nullptr);

  rgw_obj_key obj_key("test_object_key");
  std::unique_ptr<rgw::sal::Object> object;
  object = bucket->get_object(obj_key);
  ASSERT_NE(object, nullptr);
  EXPECT_EQ(object->get_key(), obj_key);

  // Check the type of the returned object.
  auto mdo_object = dynamic_cast<rgw::sal::MDOffloadObject*>(object.get());
  ASSERT_NE(mdo_object, nullptr);
  // Check the type of the underlying object.
  auto fake_object = dynamic_cast<akamai::fake::FakeObject*>(mdo_object->get_next());
  ASSERT_NE(fake_object, nullptr);
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_CreateBucketSucceeds)
{
  rgw_user u;
  u.id = "test_user";
  rgw_bucket b;
  b.name = "test_bucket";
  b.bucket_id = akamai::fake::stable_uuid_for_bucket_name(b.name);
  rgw_placement_rule placement {};
  std::string swift_ver_location {};
  rgw::sal::Attrs attrs;
  bufferlist bl;
  bl.append("test_value");
  attrs["test_attr"] = bl;
  RGWBucketInfo binfo;
  obj_version objv;
  RGWEnv env;
  req_info req(g_ceph_context, &env);
  // This receives the created bucket.
  std::unique_ptr<rgw::sal::Bucket> bucket;

  auto user = filter_->get_user(u);
  ASSERT_NE(user, nullptr);

  // When setting up the gRPC client request, we need the user ID.
  auto ret = user->create_bucket(this,
      b,
      "",
      placement,
      swift_ver_location,
      nullptr,
      RGWAccessControlPolicy {},
      attrs,
      binfo,
      objv,
      false,
      false,
      nullptr,
      req,
      &bucket,
      null_yield);
  ASSERT_GE(ret, 0);
  ASSERT_NE(bucket, nullptr);
  EXPECT_EQ(bucket->get_name(), b.name);
  EXPECT_EQ(bucket->get_bucket_id(), b.bucket_id);

  // Check the type of the returned bucket.
  auto mdo_bucket = dynamic_cast<rgw::sal::MDOffloadBucket*>(bucket.get());
  ASSERT_NE(mdo_bucket, nullptr);
  // Check the type of the underlying bucket.
  auto fake_bucket = dynamic_cast<akamai::fake::FakeBucket*>(mdo_bucket->get_next());
  ASSERT_NE(fake_bucket, nullptr);
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_fixture_get_bucket_Succeeds)
{
  std::unique_ptr<rgw::sal::MDOffloadBucket> mdo_bucket;
  fixture_get_bucket("test_bucket", &mdo_bucket);
  ASSERT_TRUE(mdo_bucket);
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_fixture_get_object_1_Succeeds)
{
  std::unique_ptr<rgw::sal::MDOffloadBucket> mdo_bucket;
  fixture_get_bucket("test_bucket", &mdo_bucket);

  rgw_obj_key obj_key("test_object_key");
  std::unique_ptr<rgw::sal::MDOffloadObject> mdo_object;
  fixture_get_object(mdo_bucket.get(), obj_key, &mdo_object);
  ASSERT_TRUE(mdo_object);
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_fixture_get_object_2_Succeeds)
{
  rgw_obj_key obj_key("test_object_key");
  std::unique_ptr<rgw::sal::MDOffloadObject> mdo_object;
  fixture_get_object("test_bucket", obj_key, &mdo_object);
  ASSERT_TRUE(mdo_object);
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Bucket_get_attr_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for Attrs&.
  rgw::sal::Attrs empty_attrs;
  DefaultValue<::rgw::sal::Attrs&>::Set(empty_attrs);

  std::unique_ptr<rgw::sal::MDOffloadBucket> mdo_bucket;
  fixture_get_bucket("test_bucket", &mdo_bucket);
  ASSERT_NE(mdo_bucket, nullptr);

  auto fake_bucket = dynamic_cast<akamai::fake::FakeBucket*>(mdo_bucket->get_next());
  ASSERT_NE(fake_bucket, nullptr);

  // Calling the invalid function directly must throw.
  ASSERT_THROW(fake_bucket->get_attrs(), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    mdo_bucket->get_attrs();
  });
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Bucket_set_attr_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for Attrs&.
  rgw::sal::Attrs empty_attrs;
  DefaultValue<::rgw::sal::Attrs&>::Set(empty_attrs);

  std::unique_ptr<rgw::sal::MDOffloadBucket> mdo_bucket;
  fixture_get_bucket("test_bucket", &mdo_bucket);
  ASSERT_NE(mdo_bucket, nullptr);

  auto fake_bucket = dynamic_cast<akamai::fake::FakeBucket*>(mdo_bucket->get_next());
  ASSERT_NE(fake_bucket, nullptr);

  // Calling the invalid function directly must throw.
  ASSERT_THROW(fake_bucket->set_attrs(empty_attrs), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    mdo_bucket->set_attrs(empty_attrs);
  });
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Bucket_merge_and_store_attrs_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for Attrs&.
  rgw::sal::Attrs empty_attrs;
  DefaultValue<::rgw::sal::Attrs&>::Set(empty_attrs);

  std::unique_ptr<rgw::sal::MDOffloadBucket> mdo_bucket;
  fixture_get_bucket("test_bucket", &mdo_bucket);
  ASSERT_NE(mdo_bucket, nullptr);

  auto fake_bucket = dynamic_cast<akamai::fake::FakeBucket*>(mdo_bucket->get_next());
  ASSERT_NE(fake_bucket, nullptr);

  // Calling the invalid function directly must throw.
  ASSERT_THROW(fake_bucket->merge_and_store_attrs(this, empty_attrs, null_yield), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    // When setting up the gRPC client request, we need the bucket name.
    EXPECT_EQ(mdo_bucket->get_name(), "test_bucket");
    mdo_bucket->merge_and_store_attrs(this, empty_attrs, null_yield);
  });
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_User_create_bucket_MustSendEmptyAttrsToFakeDriver)
{
  std::unique_ptr<rgw::sal::MDOffloadBucket> mdo_bucket;
  rgw_user u;
  u.id = "test_user";
  rgw_bucket b;
  b.name = "test_bucket";
  b.bucket_id = akamai::fake::stable_uuid_for_bucket_name(b.name);
  rgw_placement_rule placement {};
  std::string swift_ver_location {};
  rgw::sal::Attrs nonempty_attrs;
  bufferlist bl;
  bl.append("test_value");
  nonempty_attrs["test_attr"] = bl;
  RGWBucketInfo binfo;
  obj_version objv;
  RGWEnv env;
  req_info req(g_ceph_context, &env);
  // This receives the created bucket.
  std::unique_ptr<rgw::sal::Bucket> bucket;

  auto user = filter_->get_user(u);
  ASSERT_NE(user, nullptr);

  // When setting up the gRPC client request, we need the user ID.
  auto ret = user->create_bucket(this,
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
      &bucket,
      null_yield);
  ASSERT_GE(ret, 0);
  ASSERT_NE(bucket, nullptr);
  EXPECT_EQ(bucket->get_name(), b.name);
  EXPECT_EQ(bucket->get_bucket_id(), b.bucket_id);

  // Check the type of the returned bucket.
  auto mdo_bucket_check = dynamic_cast<rgw::sal::MDOffloadBucket*>(bucket.get());
  ASSERT_NE(mdo_bucket_check, nullptr);
  // Check the type of the underlying bucket.
  auto fake_bucket = dynamic_cast<akamai::fake::FakeBucket*>(mdo_bucket_check->get_next());
  ASSERT_NE(fake_bucket, nullptr);

  // The attrs passed to the fake driver must be empty.
  fake_bucket->set_throw_on_invalid(false); // Disable throwing for this check.
  EXPECT_TRUE(fake_bucket->get_attrs().empty());
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Object_get_obj_attrs_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  std::unique_ptr<rgw::sal::MDOffloadObject> mdo_object;
  rgw_obj_key obj_key("test_object_key");
  fixture_get_object("test_bucket", obj_key, &mdo_object);
  ASSERT_NE(mdo_object, nullptr);

  auto fake_object = dynamic_cast<akamai::fake::FakeObject*>(mdo_object->get_next());
  ASSERT_NE(fake_object, nullptr);

  // Calling the invalid function directly must throw.
  ASSERT_THROW(fake_object->get_obj_attrs(null_yield, this, nullptr), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    mdo_object->get_obj_attrs(null_yield, this, nullptr);
  });
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Object_set_obj_attrs_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  std::unique_ptr<rgw::sal::MDOffloadObject> mdo_object;
  rgw_obj_key obj_key("test_object_key");
  fixture_get_object("test_bucket", obj_key, &mdo_object);
  ASSERT_NE(mdo_object, nullptr);

  auto fake_object = dynamic_cast<akamai::fake::FakeObject*>(mdo_object->get_next());
  ASSERT_NE(fake_object, nullptr);

  // Calling the invalid function directly must throw.
  ASSERT_THROW(fake_object->set_obj_attrs(this, nullptr, nullptr, null_yield), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    rgw::sal::Attrs setattrs;
    rgw::sal::Attrs delattrs;
    mdo_object->set_obj_attrs(this, &setattrs, &delattrs, null_yield);
  });
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Object_modify_obj_attrs_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  std::unique_ptr<rgw::sal::MDOffloadObject> mdo_object;
  rgw_obj_key obj_key("test_object_key");
  fixture_get_object("test_bucket", obj_key, &mdo_object);
  ASSERT_NE(mdo_object, nullptr);

  auto fake_object = dynamic_cast<akamai::fake::FakeObject*>(mdo_object->get_next());
  ASSERT_NE(fake_object, nullptr);

  // Calling the invalid function directly must throw.
  bufferlist bl;
  bl.append("test_value");
  ASSERT_THROW(fake_object->modify_obj_attrs("test_attr", bl, null_yield, this), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    mdo_object->modify_obj_attrs("test_attr", bl, null_yield, this);
  });
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Object_delete_obj_attrs_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  std::unique_ptr<rgw::sal::MDOffloadObject> mdo_object;
  rgw_obj_key obj_key("test_object_key");
  fixture_get_object("test_bucket", obj_key, &mdo_object);
  ASSERT_NE(mdo_object, nullptr);

  auto fake_object = dynamic_cast<akamai::fake::FakeObject*>(mdo_object->get_next());
  ASSERT_NE(fake_object, nullptr);

  // Calling the invalid function directly must throw.
  ASSERT_THROW(fake_object->delete_obj_attrs(this, "test_attr", null_yield), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    mdo_object->delete_obj_attrs(this, "test_attr", null_yield);
  });
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Object_get_attrs_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for rgw::sal::Attrs&.
  rgw::sal::Attrs empty_attrs;
  DefaultValue<rgw::sal::Attrs&>::Set(empty_attrs);

  std::unique_ptr<rgw::sal::MDOffloadObject> mdo_object;
  rgw_obj_key obj_key("test_object_key");
  fixture_get_object("test_bucket", obj_key, &mdo_object);
  ASSERT_NE(mdo_object, nullptr);

  auto fake_object = dynamic_cast<akamai::fake::FakeObject*>(mdo_object->get_next());
  ASSERT_NE(fake_object, nullptr);

  // Calling the invalid function directly must throw.
  ASSERT_THROW(fake_object->get_attrs(), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    mdo_object->get_attrs();
  });
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Object_set_attrs_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for int.
  DefaultValue<int>::Set(0);

  std::unique_ptr<rgw::sal::MDOffloadObject> mdo_object;
  rgw_obj_key obj_key("test_object_key");
  fixture_get_object("test_bucket", obj_key, &mdo_object);
  ASSERT_NE(mdo_object, nullptr);

  auto fake_object = dynamic_cast<akamai::fake::FakeObject*>(mdo_object->get_next());
  ASSERT_NE(fake_object, nullptr);

  // Calling the invalid function directly must throw.
  rgw::sal::Attrs attrs;
  ASSERT_THROW(fake_object->set_attrs(attrs), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    mdo_object->set_attrs(attrs);
  });
}

TEST_F(RGWMDOffloadFakeDriverFixture, WithFakeDriver_Object_has_attrs_MustNotCallFakeDriver)
{
  using namespace ::testing;

  // Set up a default return value for bool.
  DefaultValue<bool>::Set(false);

  std::unique_ptr<rgw::sal::MDOffloadObject> mdo_object;
  rgw_obj_key obj_key("test_object_key");
  fixture_get_object("test_bucket", obj_key, &mdo_object);
  ASSERT_NE(mdo_object, nullptr);

  auto fake_object = dynamic_cast<akamai::fake::FakeObject*>(mdo_object->get_next());
  ASSERT_NE(fake_object, nullptr);

  // Calling the invalid function directly must throw.
  ASSERT_THROW(fake_object->has_attrs(), akamai::fake::FakeDriverInvalidOperationException);
  // Calling via the filter MUST NOT throw.
  ASSERT_NO_THROW({
    mdo_object->has_attrs();
  });
}

/* #endregion Fake */

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
