/**
 * @brief gRPC test client and server for interacting with the MDOffload service.
 *
 */

#pragma once

#include <grpcpp/grpcpp.h>

#include "include/buffer.h"

#include "mdoffload/v1/mdoffload.grpc.pb.h"

namespace akamai::test {

namespace mdo = ::mdoffload::v1;
class MDOffloadClient {

private:
  std::unique_ptr<mdo::MDOffloadService::Stub> stub_;

public:
  MDOffloadClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(mdo::MDOffloadService::NewStub(channel))
  {
  }

  // Bucket attribute RPCs
  grpc::Status GetBucketAttributes(const mdo::GetBucketAttributesRequest& request,
      mdo::GetBucketAttributesResponse* response)
  {
    grpc::ClientContext ctx;
    return stub_->GetBucketAttributes(&ctx, request, response);
  }

  grpc::Status SetBucketAttributes(const mdo::SetBucketAttributesRequest& request,
      mdo::SetBucketAttributesResponse* response)
  {
    grpc::ClientContext ctx;
    return stub_->SetBucketAttributes(&ctx, request, response);
  }

  // Object attribute RPCs
  grpc::Status GetObjectAttributes(const mdo::GetObjectAttributesRequest& request,
      mdo::GetObjectAttributesResponse* response)
  {
    grpc::ClientContext ctx;
    return stub_->GetObjectAttributes(&ctx, request, response);
  }

  grpc::Status SetObjectAttributes(const mdo::SetObjectAttributesRequest& request,
      mdo::SetObjectAttributesResponse* response)
  {
    grpc::ClientContext ctx;
    return stub_->SetObjectAttributes(&ctx, request, response);
  }

}; // class MDOffloadClient

class Attributes {
private:
  using attr_type = std::map<std::string, bufferlist>;
  attr_type attrs_;

public:
  attr_type& get_all()
  {
    return attrs_;
  }
  void set(const std::string& key, const bufferlist& value)
  {
    attrs_[key] = value;
  }
  void set(const std::string& key, const std::string& value)
  {
    bufferlist bl;
    bl.append(value);
    attrs_[key] = bl;
  }
  void modify(const std::map<std::string, std::string>& to_add, const std::map<std::string, std::string>& to_del)
  {
    for (const auto& [key, value] : to_add) {
      set(key, value);
    }
    for (const auto& [key, value] : to_del) {
      del(key);
    }
  }
  std::optional<bufferlist> get(const std::string& key) const
  {
    auto it = attrs_.find(key);
    if (it == attrs_.end()) {
      return std::nullopt;
    }
    return it->second;
  }
  bool exists(const std::string& key) const
  {
    return attrs_.find(key) != attrs_.end();
  }
  void del(const std::string& key)
  {
    attrs_.erase(key);
  }
};

// Unique key for an object: object key + instance ID. We'll need operator< to
// use this as a map key.
struct ObjectKey {
  std::string object_key;
  std::string instance_id;

  ObjectKey(const std::string& key, const std::string& instance)
      : object_key(key)
      , instance_id(instance)
  {
  }
  bool operator<(const ObjectKey& other) const
  {
    return std::tie(object_key, instance_id) < std::tie(other.object_key, other.instance_id);
  }
}; // ObjectKey

class Object {
  ObjectKey key_;
  Attributes attributes_;

public:
  Object(const ObjectKey& key)
      : key_(key)
  {
  }
  Attributes& attrs()
  {
    return attributes_;
  }
};

class Objects {
private:
  std::map<ObjectKey, Object> objects_;

public:
  std::optional<Object*> get(const std::string& object_key, const std::string& instance_id, bool create_if_missing = false)
  {
    ObjectKey key(object_key, instance_id);
    auto it = objects_.find(key);
    if (it == objects_.end()) {
      if (create_if_missing) {
        auto [new_it, inserted] = objects_.emplace(key, Object { key });
        return &new_it->second;
      } else {
        return std::nullopt;
      }
    }
    return &it->second;
  }
  bool exists(const std::string& object_key, const std::string& instance_id) const
  {
    return objects_.find(ObjectKey(object_key, instance_id)) != objects_.end();
  }
  void del(const std::string& object_key, const std::string& instance_id)
  {
    objects_.erase(ObjectKey(object_key, instance_id));
  }
};

class Bucket {
  std::string id_;
  Attributes attributes_;
  Objects objects_;

public:
  Bucket(const std::string& id)
      : id_(id)
  {
  }

  Attributes& attrs()
  {
    return attributes_;
  }
  Objects& objects()
  {
    return objects_;
  }
};
class Buckets {
private:
  std::map<std::string, Bucket> buckets_;

public:
  std::optional<Bucket*> get(const std::string& bucket_id, bool create_if_missing = false)
  {
    auto it = buckets_.find(bucket_id);
    if (it == buckets_.end()) {
      if (create_if_missing) {
        auto [new_it, inserted] = buckets_.emplace(bucket_id, Bucket { bucket_id });
        return &new_it->second;
      } else {
        return std::nullopt;
      }
    }
    return &it->second;
  }

  bool exists(const std::string& bucket_id) const
  {
    return buckets_.find(bucket_id) != buckets_.end();
  }

  void del(const std::string& bucket_id)
  {
    buckets_.erase(bucket_id);
  }
};

class MDOffloadServiceImpl final : public mdo::MDOffloadService::Service {

private:
  Buckets buckets_;

  bool create_if_missing_ = false;

public:
  MDOffloadServiceImpl() = default;
  ~MDOffloadServiceImpl() override = default;

  Buckets& buckets()
  {
    return buckets_;
  }
  bool create_if_missing()
  {
    return create_if_missing_;
  }
  void set_create_if_missing(bool val)
  {
    create_if_missing_ = val;
  }

public:
  grpc::Status GetBucketAttributes(grpc::ServerContext* /*context*/, const mdoffload::v1::GetBucketAttributesRequest* req, mdoffload::v1::GetBucketAttributesResponse* resp) override
  {
    auto bucket = buckets().get(req->bucket_id(), create_if_missing());
    if (!bucket.has_value()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Bucket not found");
    }
    auto attr = resp->mutable_attributes();
    for (const auto& [key, value] : (*bucket)->attrs().get_all()) {
      // get_all() returns map<string, bufferlist>.
      (*attr)[key] = value.to_str();
    }
    return grpc::Status::OK;
  }
  grpc::Status SetBucketAttributes(grpc::ServerContext* /*context*/, const mdoffload::v1::SetBucketAttributesRequest* req, mdoffload::v1::SetBucketAttributesResponse* resp) override
  {
    auto bucket = buckets().get(req->bucket_id(), create_if_missing());
    if (!bucket.has_value()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Bucket not found");
    }
    for (auto& [key, value] : req->attributes_to_add()) {
      (*bucket)->attrs().set(key, value);
    }
    for (auto& key : req->attributes_to_delete()) {
      (*bucket)->attrs().del(key);
    }
    return grpc::Status::OK;
  }
  grpc::Status GetObjectAttributes(grpc::ServerContext* /*context*/, const mdoffload::v1::GetObjectAttributesRequest* req, mdoffload::v1::GetObjectAttributesResponse* resp) override
  {
    auto bucket = buckets().get(req->bucket_id(), create_if_missing());
    if (!bucket.has_value()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Bucket not found");
    }
    auto object = (*bucket)->objects().get(req->object_key(), req->object_instance_id(), create_if_missing());
    if (!object.has_value()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Object not found");
    }
    for (const auto& [key, value] : (*object)->attrs().get_all()) {
      (*resp->mutable_attributes())[key] = value.to_str();
    }
    return grpc::Status::OK;
  }
  grpc::Status SetObjectAttributes(grpc::ServerContext* /*context*/, const mdoffload::v1::SetObjectAttributesRequest* req, mdoffload::v1::SetObjectAttributesResponse* resp) override
  {
    auto bucket = buckets().get(req->bucket_id(), create_if_missing());
    if (!bucket.has_value()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Bucket not found");
    }
    auto object = (*bucket)->objects().get(req->object_key(), req->object_instance_id(), create_if_missing());
    if (!object.has_value()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Object not found");
    }
    for (auto& [key, value] : req->attributes_to_add()) {
      (*object)->attrs().set(key, value);
    }
    for (auto& key : req->attributes_to_delete()) {
      (*object)->attrs().del(key);
    }
    return grpc::Status::OK;
  }
}; // class MDOffloadServiceImpl

} // namespace ::akamai::test
