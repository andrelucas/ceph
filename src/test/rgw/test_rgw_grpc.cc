/**
 * @file test_rgw_grpc.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Unit tests for gRPC integration in RGW.
 * @version 0.1
 * @date 2023-11-07
 *
 * @copyright Copyright (c) 2023
 *
 */

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <gtest/gtest.h>

// It's safe to include Abseil, as that's part of gRPC.
#include <absl/random/random.h>

#include "rgw/auth/v1/auth.grpc.pb.h"
#include "rgw/test/v1/test_rgw_grpc.grpc.pb.h"

using namespace ::rgw::test::v1;

static constexpr uint16_t port_base = 58000;
static constexpr uint16_t port_range = 2000;

static uint16_t random_port()
{
  absl::BitGen bitgen;
  uint16_t rand = absl::Uniform(bitgen, 0u, port_range);
  return port_base + rand;
}

/**
 * @brief Minimal gRPC client wrapper. Initialised with a grpc::Channel.
 *
 *
 */
class TestClient {
private:
  std::unique_ptr<RgwGrpcTestService::Stub> stub_;

public:
  TestClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(RgwGrpcTestService::NewStub(channel))
  {
  }

  std::optional<std::string> Ping(const std::string message)
  {
    grpc::ClientContext context;
    PingRequest req;
    PingResponse resp;
    req.set_message("foo");
    grpc::Status status = stub_->Ping(&context, req, &resp);
    if (!status.ok()) {
      return std::nullopt;
    }
    return std::make_optional(resp.message());
  }
};

class TestImpl final : public RgwGrpcTestService::Service {
  grpc::Status Ping(grpc::ServerContext* context, const PingRequest* request, PingResponse* response) override
  {
    response->set_message(request->message());
    return grpc::Status::OK;
  }
};

// gtest fixture. Doesn't implicitly start the server, you have to call
// start_test(). It will call stop_server() in TearDown, that feels relatively
// safe. It's safe to call start_server() and stop_server() multiple times.
class TestGrpcService : public ::testing::Test {
protected:
  std::thread server_thread;
  // Used to prevent fast startup/shutdown problems. (The Null test.)
  std::atomic<bool> initialising = false;
  // True if the server is actually running (in Wait()).
  std::atomic<bool> running = false;
  uint16_t server_port;
  std::string server_address = "127.0.0.1:58000"; // XXX randomise!
  std::unique_ptr<grpc::Server> server;

  // Don't start the server - some tests might want a chance to see what
  // happens without a server.
  void SetUp() override
  {
    server_port = random_port();
    server_address = fmt::format("127.0.0.1:{}", server_port);
  }

  // Will stop the server. There's no situation where we want it left around.
  void TearDown() override
  {
    stop_server();
  }

  // Fire up a gRPC server for TestImpl in a separate thread, setting some
  // atomics in the instance to let other methods check on our progress.
  //
  void start_server()
  {
    if (initialising || running) {
      return;
    }
    initialising = true;
    server_thread = std::move(std::thread([this]() {
      TestImpl service;
      grpc::ServerBuilder builder;
      builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
      builder.RegisterService(&service);
      server = builder.BuildAndStart();
      if (!server) {
        fmt::print(stderr, "Failed to BuildAndStart() for {}\n", server_address);
        initialising = false;
        return;
      }
      running = true;
      initialising = false;
      server->Wait();
      running = false;
    }));
  }

  void stop_server()
  {
    while (initialising)
      ;
    if (running && server) {
      server->Shutdown();
    }
    if (server_thread.joinable()) {
      server_thread.join();
    }
  }
};

TEST_F(TestGrpcService, Null)
{
}

// Make sure start_server() is idempotent.
TEST_F(TestGrpcService, MetaStart)
{
  start_server();
  for (int n = 0; n < 1000; n++) {
    start_server();
  }
  stop_server();
}

// Make sure stop_server() is idempotent.
TEST_F(TestGrpcService, MetaStop)
{
  start_server();
  for (int n = 0; n < 1000; n++) {
    stop_server();
  }
}

TEST_F(TestGrpcService, PingWorksWithServer)
{
  start_server();
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  TestClient client { channel };
  auto message = client.Ping("foo");
  EXPECT_TRUE(message.has_value()) << "Ping failed";
  if (message.has_value()) {
    EXPECT_EQ(*message, "foo");
  }
  stop_server();
}

TEST_F(TestGrpcService, PingFailsWithNoServer)
{
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  TestClient client { channel };
  auto message = client.Ping("foo");
  EXPECT_FALSE(message.has_value()) << "Ping failed";
}
