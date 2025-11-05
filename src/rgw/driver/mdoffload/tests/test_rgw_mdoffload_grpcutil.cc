/**
 * @file test_rgw_mdoffload_grpcutil.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Test gRPC client and server for MDOffloadService.
 * @version 0.1
 * @date 2025-11-05
 *
 * @copyright Copyright (c) 2025
 *
 */

#include <grpc/grpc.h>
#include <gmock/gmock.h>

#include "test_rgw_grpc_util.h"
#include "test_rgw_mdoffload_util.h"

#include "test_rgw_mdoffload_grpcutil.h"

namespace {

namespace mdo = ::mdoffload::v1;

class MDOffloadGrpcStandaloneServer : public ::testing::Test, public akamai::test::CephGtestLogAdapter {
public:
  using server_type = GRPCTestServer<akamai::test::MDOffloadServiceImpl>;

protected:
  server_type server_;

  void TearDown() override { server_.stop(); }
  server_type& server() { return server_; }
}; // class MDOffloadGrpcMock

// Make sure the server objects are properly created and destroyed.
TEST_F(MDOffloadGrpcStandaloneServer, Null)
{
}

TEST_F(MDOffloadGrpcStandaloneServer, MetaStart)
{
  server().start();
  for (int n = 0; n < 1000; n++) {
    server().start();
  }
  server().stop();
}

TEST_F(MDOffloadGrpcStandaloneServer, MetaStop)
{
  server().start();
  for (int n = 0; n < 1000; n++) {
    server().stop();
  }
}

TEST_F(MDOffloadGrpcStandaloneServer, MetaRoundTrip)
{
  server().start();

  auto channel = grpc::CreateChannel(server().address(), grpc::InsecureChannelCredentials());
  akamai::test::MDOffloadClient client(channel);

  mdo::GetBucketAttributesRequest req;
  mdo::GetBucketAttributesResponse resp;

  server().instance()->set_create_if_missing(true); // Without this, GetBucketAttributes will fail.
  req.set_bucket_id("test_bucket_id");
  auto s = client.GetBucketAttributes(req, &resp);
  ASSERT_TRUE(s.ok());

  server().stop();
}

TEST_F(MDOffloadGrpcStandaloneServer, MetaBucketContainer)
{
  server().start();
  auto instance = server().instance();
  auto buckets = instance->buckets();

  auto opt_bucket = buckets.get("test_bucket", false);
  ASSERT_FALSE(opt_bucket.has_value());
  opt_bucket = buckets.get("test_bucket", true);
  ASSERT_TRUE(opt_bucket.has_value());

  auto bucket = opt_bucket.value();
  EXPECT_TRUE(bucket->attrs().get_all().empty()) << "New bucket should have no attributes";
  bucket->attrs().set("attr1", "value1");
  EXPECT_EQ(bucket->attrs().get_all().size(), 1u);
  EXPECT_TRUE(bucket->attrs().exists("attr1"));
  EXPECT_EQ(bucket->attrs().get("attr1").value().to_str(), "value1");
  bucket->attrs().del("attr1");
  EXPECT_FALSE(bucket->attrs().exists("attr1"));
  EXPECT_TRUE(bucket->attrs().get_all().empty()) << "Bucket should have no attributes after deletion";

  buckets.del("test_bucket");
  EXPECT_FALSE(buckets.exists("test_bucket"));

  server().stop();
}

TEST_F(MDOffloadGrpcStandaloneServer, MetaObjectContainer)
{
  server().start();
  auto instance = server().instance();

  auto buckets = instance->buckets();
  auto opt_bucket = buckets.get("test_bucket", true);
  ASSERT_TRUE(opt_bucket.has_value());
  auto bucket = opt_bucket.value();

  auto& objects = bucket->objects();
  auto opt_object = objects.get("test_object", "", false);
  ASSERT_FALSE(opt_object.has_value());
  opt_object = objects.get("test_object", "", true);
  ASSERT_TRUE(opt_object.has_value());
  auto object = opt_object.value();

  EXPECT_TRUE(object->attrs().get_all().empty()) << "New object should have no attributes";
  object->attrs().set("attr1", "value1");
  EXPECT_EQ(object->attrs().get_all().size(), 1u);
  EXPECT_TRUE(object->attrs().exists("attr1"));
  EXPECT_EQ(object->attrs().get("attr1").value().to_str(), "value1");
  object->attrs().del("attr1");
  EXPECT_FALSE(object->attrs().exists("attr1"));
  EXPECT_TRUE(object->attrs().get_all().empty()) << "Object should have no attributes after deletion";

  objects.del("test_object", "");
  EXPECT_FALSE(objects.exists("test_object", ""));

  server().stop();
}

TEST_F(MDOffloadGrpcStandaloneServer, TestServerGetSetBucketAttributesBasics)
{
  server().start();
  auto instance = server().instance();
  instance->set_create_if_missing(true);

  auto channel = grpc::CreateChannel(server().address(), grpc::InsecureChannelCredentials());
  akamai::test::MDOffloadClient client(channel);
  auto req = mdo::GetBucketAttributesRequest {};
  req.set_bucket_id("test_bucket_id");

  mdo::GetBucketAttributesResponse resp;
  auto s = client.GetBucketAttributes(req, &resp);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(resp.attributes_size() == 0) << "New bucket should have no attributes";

  mdo::SetBucketAttributesRequest set_req;
  mdo::SetBucketAttributesResponse set_resp;
  auto attr_add = set_req.mutable_attributes_to_add();
  auto attr_del = set_req.mutable_attributes_to_delete();

  set_req.set_bucket_id("test_bucket_id");
  (*attr_add)["attr1"] = "value1";
  s = client.SetBucketAttributes(set_req, &set_resp);
  ASSERT_TRUE(s.ok());

  s = client.GetBucketAttributes(req, &resp);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(resp.attributes_size() == 1) << "Bucket should have one attribute after setting";
  auto attr_get = resp.attributes();
  EXPECT_EQ(attr_get["attr1"], "value1");

  attr_add->clear();
  attr_del->Add("attr1");
  s = client.SetBucketAttributes(set_req, &set_resp);
  ASSERT_TRUE(s.ok());
  s = client.GetBucketAttributes(req, &resp);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(resp.attributes_size() == 0) << "Bucket should have no attributes after deletion";

  server().stop();
}

TEST_F(MDOffloadGrpcStandaloneServer, TestServerGetSetObjectAttributesBasics)
{
  server().start();
  auto instance = server().instance();
  instance->set_create_if_missing(true);

  auto channel = grpc::CreateChannel(server().address(), grpc::InsecureChannelCredentials());
  akamai::test::MDOffloadClient client(channel);
  auto req = mdo::GetObjectAttributesRequest {};
  req.set_bucket_id("test_bucket_id");
  req.set_object_key("test_object_key");
  req.set_object_instance_id("");

  mdo::GetObjectAttributesResponse resp;
  auto s = client.GetObjectAttributes(req, &resp);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(resp.attributes_size() == 0) << "New object should have no attributes";

  mdo::SetObjectAttributesRequest set_req;
  mdo::SetObjectAttributesResponse set_resp;
  auto attr_add = set_req.mutable_attributes_to_add();
  auto attr_del = set_req.mutable_attributes_to_delete();

  set_req.set_bucket_id("test_bucket_id");
  set_req.set_object_key("test_object_key");
  set_req.set_object_instance_id("");
  (*attr_add)["attr1"] = "value1";
  s = client.SetObjectAttributes(set_req, &set_resp);
  ASSERT_TRUE(s.ok());

  s = client.GetObjectAttributes(req, &resp);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(resp.attributes_size() == 1) << "Object should have one attribute after setting";
  auto attr_get = resp.attributes();
  EXPECT_EQ(attr_get["attr1"], "value1");

  attr_add->clear();
  attr_del->Add("attr1");
  s = client.SetObjectAttributes(set_req, &set_resp);
  ASSERT_TRUE(s.ok());
  s = client.GetObjectAttributes(req, &resp);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(resp.attributes_size() == 0) << "Object should have no attributes after deletion";

  server().stop();
}

}
