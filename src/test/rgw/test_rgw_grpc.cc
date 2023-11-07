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

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "rgw/test/v1/test_rgw_grpc.grpc.pb.h"

using namespace ::rgw::test::v1;

class TestImpl final : public RgwGrpcTestService::Service {
  grpc::Status Ping(grpc::ServerContext* context, const PingRequest* request, PingResponse* response) override
  {
    response->set_message(request->message());
    return grpc::Status::OK;
  }
};

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

class TestGrpcService : public ::testing::Test {
protected:
  std::thread server_thread;
  // Used to prevent fast startup/shutdown problems. (The Null test.)
  std::atomic<bool> initialising;
  // True if the server is actually running (in Wait()).
  std::atomic<bool> running;
  std::string server_address = "127.0.0.1:58000"; // XXX randomise!
  std::unique_ptr<grpc::Server> server;

  void SetUp() override
  {
    initialising = true;
    server_thread = std::move(std::thread([this]() {
      TestImpl service;
      grpc::ServerBuilder builder;
      builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
      builder.RegisterService(&service);
      server = builder.BuildAndStart();
      if (!server) {
        std::cerr << "Failed to BuildAndStart()" << std::endl;
        initialising = false;
        return;
      }
      running = true;
      initialising = false;
      server->Wait();
      running = false;
    }));
  }
  void TearDown() override
  {
    while (initialising)
      ;
    if (running && server) {
      server->Shutdown();
    }
    server_thread.join();
  }
};

TEST_F(TestGrpcService, Null)
{
}

TEST_F(TestGrpcService, Ping)
{
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  TestClient client { channel };
  auto message = client.Ping("foo");
  ASSERT_TRUE(message.has_value()) << "Ping failed";
  ASSERT_EQ(*message, "foo");
}
