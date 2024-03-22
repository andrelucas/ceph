/**
 * @file test_rgw_ubns.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Unit tests for the Unique Bucket Name Service implementation in RGW.
 * @version 0.1
 * @date 2024-03-20
 *
 * @copyright Copyright (c) 2024
 */

#include <set>
#include <shared_mutex>
#include <string>

#include <absl/random/random.h>
#include <grpcpp/support/status.h>

#include <gtest/gtest.h>

#include "rgw_ubns.h"
#include "rgw_ubns_impl.h"
#include "test_rgw_grpc_util.h"
#include "ubdb/v1/ubdb.grpc.pb.h"

#include "common/async/yield_context.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "global/global_init.h"
#include "rgw/rgw_client_io.h"
#include "rgw/rgw_common.h"

using namespace ::ubdb::v1;

namespace {

using namespace rgw;

// Minimal client for req_state.
class TestClient : public rgw::io::BasicClient {
  RGWEnv env;

protected:
  virtual int init_env(CephContext* cct) override
  {
    return 0;
  }

public:
  virtual RGWEnv& get_env() noexcept override
  {
    return env;
  }

  virtual size_t complete_request() override
  {
    return 0;
  }
};

/* #endregion */

using namespace rgw;

// Stole this from test_rgw_lua.cc. Set up a req_state s for testing.
#define DEFINE_REQ_STATE \
  RGWEnv e;              \
  req_state s(g_ceph_context, &e, 0);

class LockedSet {

public:
  using lock_t = std::shared_mutex;

private:
  std::set<std::string> set_;
  mutable std::shared_mutex lock_;

public:
  LockedSet() = default;
  ~LockedSet() = default;

  bool check_insert(const std::string& key)
  {
    std::unique_lock<lock_t> l(lock_);
    auto srch = set_.find(key);
    if (srch != set_.cend()) {
      return false;
    }
    set_.insert(key);
    return true;
  }

  bool check_erase(const std::string& key)
  {
    std::unique_lock<lock_t> l(lock_);
    auto srch = set_.find(key);
    if (srch == set_.cend()) {
      return false;
    }
    set_.erase(key);
    return true;
  }

  bool exists(const std::string& key) const
  {
    std::shared_lock<lock_t> l(lock_);
    return (set_.find(key) == set_.cend());
  }

  void clear()
  {
    std::unique_lock<lock_t> l(lock_);
    set_.clear();
  }

}; // class LockedSet

// This is hardcoded in the library, you can't configure a reconnect delay
// less than 100ms. (grpc src/core/ext/filters/client_channel/subchannel.cc
// function ParseArgsForBackoffValues().) This allows five more milliseconds.
//
constexpr int SMALLEST_RECONNECT_DELAY_MS = 105;

class TestUBNSClientImpl : public ubdb::v1::UBDBService::Service {

private:
  LockedSet buckets_;

public:
  grpc::Status AddBucketEntry(grpc::ServerContext* context,
      const AddBucketEntryRequest* request,
      AddBucketEntryResponse* response) override
  {
    if (!buckets_.check_insert(request->bucket())) {
      return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "Bucket already exists");
    }
    return grpc::Status::OK;
  }

  grpc::Status DeleteBucketEntry(grpc::ServerContext* context,
      const DeleteBucketEntryRequest* request,
      DeleteBucketEntryResponse* response) override
  {
    if (!buckets_.check_erase(request->bucket())) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Bucket not found");
    }
    return grpc::Status::OK;
  }

  // XXX UNIMPLEMENTED
  //   grpc::Status UpdateBucketEntry(grpc::ServerContext* context,
  //       const UpdateBucketEntryRequest* request,
  //       UpdateBucketEntryResponse* response) override
  //   {
  //     return grpc::Status::OK;
  //   }
}; // class TestUBNSClientImpl

class UBNSTestImplGRPCTest : public ::testing::Test {
protected:
  UBNSClientImpl uci_;
  optional_yield y_ = null_yield;
  DoutPrefix dpp_ { g_ceph_context, ceph_subsys_rgw, "unittest " };

  // This manages the test gRPC server.
  GRPCTestServer<TestUBNSClientImpl> server_;

  // Don't start the server - some tests might want a chance to see what
  // happens without a server.
  void SetUp() override
  {
  }

  void helper_init()
  {
    dpp_.get_cct()->_conf.set_val_or_die("rgw_ubns_enabled", "true");
    dpp_.get_cct()->_conf.apply_changes(nullptr);
    ASSERT_EQ(dpp_.get_cct()->_conf->rgw_ubns_enabled, true);
    // Note init() can take the server address URI, it's normally defaulted to
    // empty which means 'use the Ceph configuration'.
    ASSERT_TRUE(uci_.init(g_ceph_context, server_.address()));
  }

  // Will stop the server. There's no situation where we want it left around.
  void TearDown() override
  {
    server().stop();
  }

  /// Return the gRPC server manager instance.
  GRPCTestServer<TestUBNSClientImpl>& server() { return server_; }
}; // class UBNSTestImplGRPCTest

TEST_F(UBNSTestImplGRPCTest, Null)
{
}

// Make sure server().start() is idempotent.
TEST_F(UBNSTestImplGRPCTest, MetaStart)
{
  server().start();
  for (int n = 0; n < 1000; n++) {
    server().start();
  }
  server().stop();
}

// Make sure server().stop() is idempotent.
TEST_F(UBNSTestImplGRPCTest, MetaStop)
{
  server().start();
  for (int n = 0; n < 1000; n++) {
    server().stop();
  }
}

TEST_F(UBNSTestImplGRPCTest, AddBucketSucceeds)
{
  server().start();
  helper_init();
  TestClient cio;
  DEFINE_REQ_STATE;
  s.cio = &cio;
  auto res = uci_.add_bucket_entry(&dpp_, "foo");
  EXPECT_TRUE(res.ok()) << "single add should succeed, but got: " << res.message();
}

TEST_F(UBNSTestImplGRPCTest, AddTwiceFails)
{
  server().start();
  helper_init();
  TestClient cio;
  DEFINE_REQ_STATE;
  s.cio = &cio;
  auto res = uci_.add_bucket_entry(&dpp_, "foo");
  EXPECT_TRUE(res.ok()) << "first add should succeed, but got: " << res.message();
  res = uci_.add_bucket_entry(&dpp_, "foo");
  EXPECT_FALSE(res.ok()) << "second add of same bucket should fail";
}

TEST_F(UBNSTestImplGRPCTest, AddRemoveAddSucceeds)
{
  server().start();
  helper_init();
  TestClient cio;
  DEFINE_REQ_STATE;
  s.cio = &cio;
  auto res = uci_.add_bucket_entry(&dpp_, "foo");
  EXPECT_TRUE(res.ok()) << "first add should succeed, but got: " << res.message();
  res = uci_.delete_bucket_entry(&dpp_, "foo");
  EXPECT_TRUE(res.ok()) << "remove same bucket should succeed, but got: " << res.message();
  res = uci_.add_bucket_entry(&dpp_, "foo");
  EXPECT_TRUE(res.ok()) << "re-add of same bucket after deletion should succeed, but got: " << res.message();
}

TEST_F(UBNSTestImplGRPCTest, DeleteNonexistentFails)
{
  server().start();
  helper_init();
  TestClient cio;
  DEFINE_REQ_STATE;
  s.cio = &cio;
  auto res = uci_.delete_bucket_entry(&dpp_, "foo");
  EXPECT_FALSE(res.ok()) << "delete of nonexistent bucket should fail";
}

TEST_F(UBNSTestImplGRPCTest, SecondDeleteFails)
{
  server().start();
  helper_init();
  TestClient cio;
  DEFINE_REQ_STATE;
  s.cio = &cio;
  auto res = uci_.add_bucket_entry(&dpp_, "foo");
  EXPECT_TRUE(res.ok()) << "add should succeed, but got: " << res.message();
  res = uci_.delete_bucket_entry(&dpp_, "foo");
  EXPECT_TRUE(res.ok()) << "delete of existing bucket should succeed, but got: " << res.message();
  res = uci_.delete_bucket_entry(&dpp_, "foo");
  EXPECT_FALSE(res.ok()) << "second delete of non-nonexistent bucket should fail";
}

// Check the system doesn't fail if started with a non-functional auth server.
TEST_F(UBNSTestImplGRPCTest, ChannelRecoversFromDeadAtStartup)
{
  ceph_assert(g_ceph_context != nullptr);
  // Set everything to 1ms. As descrived for SMALLEST_RECONNECT_DELAY_MS,
  // we'll still have to wait 100ms + a few more millis for any reconnect.
  auto args = uci_.get_default_channel_args(g_ceph_context);
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 1);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 1);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1);
  // // This is an alternate means of setting the reconnect delay, but it too
  // // bounded below at 100ms by the library.
  // args.SetInt("grpc.testing.fixed_fixed_reconnect_backoff_ms", 0);
  // Program the helper's channel.
  uci_.set_channel_args(dpp_.get_cct(), args);

  helper_init();
  TestClient cio;

  DEFINE_REQ_STATE;
  s.cio = &cio;
  //   auto res = hh_.auth(&dpp_, "", t.access_key, string_to_sign, t.signature, &s, y_);
  auto res = uci_.add_bucket_entry(&dpp_, "foo");
  ASSERT_FALSE(res.ok()) << "should fail";
  //   ASSERT_EQ(res.code(), -EACCES) << "should return -EACCES";
  //   ASSERT_EQ(res.err_type(), HandoffAuthResult::error_type::TRANSPORT_ERROR) << "should return TRANSPORT_ERROR";

  server().start();
  // Wait as short a time as the library allows.
  std::this_thread::sleep_for(std::chrono::milliseconds(SMALLEST_RECONNECT_DELAY_MS));
  //   res = hh_.auth(&dpp_, "", t.access_key, string_to_sign, t.signature, &s, y_);
  res = uci_.add_bucket_entry(&dpp_, "foo");
  EXPECT_TRUE(res.ok()) << "should now succeed";
  //   EXPECT_EQ(res.err_type(), HandoffAuthResult::error_type::NO_ERROR) << "should now show no error";
}

/**
 * @brief Mock the chunks of HandoffHelperImpl we need to check that the
 * config observer is working properly.
 *
 * See the notes on HandoffConfigObserver<T> for more details. This class
 * allows us to instantiate a config observer and make sure it's responding
 * correctly to configuration changes.
 */
class MockHelperForConfigObserver final {
public:
  MockHelperForConfigObserver()
      : observer_(*this)
  {
  }
  ~MockHelperForConfigObserver() = default;

  int init(CephContext* const cct)
  {
    return 0;
  }
  grpc::ChannelArguments get_default_channel_args(CephContext* const cct)
  {
    return grpc::ChannelArguments();
  }
  void set_channel_args(CephContext* const cct, const grpc::ChannelArguments& args) { channel_args_set_ = true; }
  void set_channel_uri(CephContext* const cct, const std::string& uri) { channel_uri_ = uri; }

public:
  UBNSConfigObserver<MockHelperForConfigObserver> observer_;
  bool chunked_upload_;
  bool channel_args_set_ = false;
  std::string channel_uri_;
};

/**
 * @brief Test that the config observer is hooked up properly for runtime
 * changes to variables we care about.
 */
class TestHandoffConfigObserver : public ::testing::Test {

protected:
  void SetUp() override
  {
    ASSERT_EQ(dpp_.get_cct()->_conf->rgw_ubns_enabled, true);
    ASSERT_EQ(uci_.init(g_ceph_context), 0);
  }

  MockHelperForConfigObserver uci_;
  DoutPrefix dpp_ { g_ceph_context, ceph_subsys_rgw, "unittest " };
};

TEST_F(TestHandoffConfigObserver, Null)
{
}

// Test that the config change propagates to the helper. We're not parsing the
// individual arg setting, that would mean essentially recreating the helper's
// code in the mock which is pointless.
//
// In all the test cases we'll call handle_conf_change() directly. I had
// problems getting the observer to work reliably in unit tests, whether I
// just relied on 'automatic' change application, or if I directly called
// conf.apply_changes(). It doesn't really matter - what we're testing here is
// that if handle_conf_change() is called properly, then the configuration
// will flow through to the helperimpl.
//
TEST_F(TestHandoffConfigObserver, GRPCChannelArgs)
{
  // Parameters we'll 'change'.
  std::set<std::string> changed;

  std::set<std::string> param = {
    "rgw_ubns_grpc_arg_initial_reconnect_backoff_ms",
    "rgw_ubns_grpc_arg_min_reconnect_backoff_ms",
    "rgw_ubns_grpc_arg_max_reconnect_backoff_ms"
  };

  auto cct = dpp_.get_cct();
  auto conf = cct->_conf;

  for (const auto& p : param) {
    uci_.channel_args_set_ = false;

    conf.set_val_or_die(p, "1001");
    changed.clear();
    changed.emplace(p);
    uci_.observer_.handle_conf_change(conf, changed);
    ASSERT_TRUE(uci_.channel_args_set_);
  }
}

TEST_F(TestHandoffConfigObserver, GRPCURI)
{
  // Parameters we'll 'change'.
  std::set<std::string> changed { "rgw_ubns_grpc_uri" };

  auto cct = dpp_.get_cct();
  auto conf = cct->_conf;

  conf->rgw_ubns_grpc_uri = "foo";
  uci_.observer_.handle_conf_change(conf, changed);
  ASSERT_EQ(uci_.channel_uri_, "foo");
}

} // namespace

// main() cribbed from test_http_manager.cc

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  // Let the caller change the library debug level.
  if (std::getenv("TEST_DEBUG")) {
    std::string err;
    int level = strict_strtol(std::getenv("TEST_DEBUG"), 10, &err);
    if (err.empty()) {
      g_ceph_context->_conf->subsys.set_log_level(ceph_subsys_rgw, std::min(level, 30));
    }
  }

  ::testing::InitGoogleTest(&argc, argv);
  int r = RUN_ALL_TESTS();

  return r;
}
