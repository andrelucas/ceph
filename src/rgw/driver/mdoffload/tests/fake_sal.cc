// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/**
 * @file fake_sal.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Fake rgw::sal implementations for unit tests.
 * @version 0.1
 * @date 2025-11-17
 *
 * @copyright Copyright (c) 2025 Akamai Inc.
 *
 */

#include "fake_sal.h"

namespace akamai::fake {

FakeMultipartPart::FakeMultipartPart(uint32_t num, uint64_t size, std::string etag)
    : num_{num}, size_{size}, etag_{std::move(etag)} {}

uint32_t FakeMultipartPart::get_num() { return num_; }

uint64_t FakeMultipartPart::get_size() { return size_; }

const std::string& FakeMultipartPart::get_etag() { return etag_; }

ceph::real_time& FakeMultipartPart::get_mtime() { return mtime_; }

FakeObject::FakeObject() : bucket_key_{}, key_{}, obj_(bucket_key_, key_) {}

FakeObject::FakeObject(const rgw_bucket& bucket, const rgw_obj_key& key)
    : bucket_key_(bucket), key_(key), obj_(bucket, key) {}

int FakeObject::delete_object(const DoutPrefixProvider*, optional_yield, uint32_t) {
  return 0;
}

int FakeObject::delete_obj_aio(const DoutPrefixProvider*, RGWObjState*, Completions*,
                               bool, optional_yield) {
  return 0;
}

int FakeObject::copy_object(User*, req_info*, const rgw_zone_id&, rgw::sal::Object*,
                            rgw::sal::Bucket*, rgw::sal::Bucket*,
                            const rgw_placement_rule&, ceph::real_time*,
                            ceph::real_time*, const ceph::real_time*,
                            const ceph::real_time*, bool, const char*, const char*,
                            AttrsMod, bool, Attrs&, RGWObjCategory, uint64_t,
                            boost::optional<ceph::real_time>, std::string*,
                            std::string*, std::string*, void (*)(off_t, void*), void*,
                            const DoutPrefixProvider*, optional_yield) {
  return 0;
}

RGWAccessControlPolicy& FakeObject::get_acl() { return acl_; }

int FakeObject::set_acl(const RGWAccessControlPolicy& acl) {
  acl_ = acl;
  return 0;
}

void FakeObject::set_atomic() { atomic_ = true; }

bool FakeObject::is_atomic() { return atomic_; }

void FakeObject::set_prefetch_data() { prefetch_ = true; }

bool FakeObject::is_prefetch_data() { return prefetch_; }

void FakeObject::set_compressed() { compressed_ = true; }

bool FakeObject::is_compressed() { return compressed_; }

void FakeObject::invalidate() {
  attrs_.clear();
  has_attrs_ = false;
}

bool FakeObject::empty() const { return key_.empty(); }

const std::string& FakeObject::get_name() const { return key_.name; }

int FakeObject::get_obj_state(const DoutPrefixProvider*, RGWObjState**, optional_yield,
                              bool) {
  return 0;
}

int FakeObject::set_obj_attrs(const DoutPrefixProvider*, Attrs* setattrs,
                              Attrs* delattrs, optional_yield) {
  if (setattrs) {
    attrs_ = *setattrs;
    has_attrs_ = true;
  }
  if (delattrs) {
    for (const auto& entry : *delattrs) {
      attrs_.erase(entry.first);
    }
  }
  return 0;
}

int FakeObject::get_obj_attrs(optional_yield, const DoutPrefixProvider*, rgw_obj* target_obj) {
  if (target_obj) {
    *target_obj = obj_;
  }
  return 0;
}

int FakeObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                                 optional_yield, const DoutPrefixProvider*) {
  if (attr_name) {
    attrs_[attr_name] = attr_val;
    has_attrs_ = true;
  }
  return 0;
}

int FakeObject::delete_obj_attrs(const DoutPrefixProvider*, const char* attr_name,
                                 optional_yield) {
  if (attr_name) {
    attrs_.erase(attr_name);
  }
  return 0;
}

bool FakeObject::is_expired() { return false; }

void FakeObject::gen_rand_obj_instance_name() { key_.instance = "rand"; }

std::unique_ptr<MPSerializer> FakeObject::get_serializer(const DoutPrefixProvider*,
                                                         const std::string&) {
  return nullptr;
}

int FakeObject::transition(Bucket*, const rgw_placement_rule&, const real_time&,
                           uint64_t, const DoutPrefixProvider*, optional_yield, uint32_t) {
  return 0;
}

int FakeObject::transition_to_cloud(Bucket*, rgw::sal::PlacementTier*,
                                    rgw_bucket_dir_entry&, std::set<std::string>&,
                                    CephContext*, bool, const DoutPrefixProvider*,
                                    optional_yield) {
  return 0;
}

bool FakeObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) {
  return r1 == r2;
}

int FakeObject::dump_obj_layout(const DoutPrefixProvider*, optional_yield,
                                Formatter*) {
  return 0;
}

Attrs& FakeObject::get_attrs() { return attrs_; }

const Attrs& FakeObject::get_attrs() const { return attrs_; }

int FakeObject::set_attrs(Attrs a) {
  attrs_ = std::move(a);
  has_attrs_ = true;
  return 0;
}

bool FakeObject::has_attrs() { return has_attrs_; }

ceph::real_time FakeObject::get_mtime() const { return mtime_; }

uint64_t FakeObject::get_obj_size() const { return size_; }

Bucket* FakeObject::get_bucket() const { return bucket_; }

void FakeObject::set_bucket(Bucket* b) { bucket_ = b; }

std::string FakeObject::get_hash_source() {
  return hash_source_.empty() ? key_.name : hash_source_;
}

void FakeObject::set_hash_source(std::string s) { hash_source_ = std::move(s); }

std::string FakeObject::get_oid() const { return key_.name; }

bool FakeObject::get_delete_marker() { return delete_marker_; }

bool FakeObject::get_in_extra_data() { return in_extra_data_; }

void FakeObject::set_in_extra_data(bool i) { in_extra_data_ = i; }

void FakeObject::set_obj_size(uint64_t s) { size_ = s; }

void FakeObject::set_name(const std::string& n) {
  key_.name = n;
  obj_.key.name = n;
}

void FakeObject::set_key(const rgw_obj_key& k) {
  key_ = k;
  obj_.set_key(k);
}

rgw_obj FakeObject::get_obj() const { return obj_; }

int FakeObject::swift_versioning_restore(bool& restored, const DoutPrefixProvider*) {
  restored = false;
  return -ENOTSUP;
}

int FakeObject::swift_versioning_copy(const DoutPrefixProvider*, optional_yield) {
  return -ENOTSUP;
}

std::unique_ptr<Object::ReadOp> FakeObject::get_read_op() { return nullptr; }

std::unique_ptr<Object::DeleteOp> FakeObject::get_delete_op() { return nullptr; }

int FakeObject::omap_get_vals(const DoutPrefixProvider*, const std::string&,
                              uint64_t, std::map<std::string, bufferlist>*, bool*,
                              optional_yield) {
  return 0;
}

int FakeObject::omap_get_all(const DoutPrefixProvider*, std::map<std::string, bufferlist>*,
                             optional_yield) {
  return 0;
}

int FakeObject::omap_get_vals_by_keys(const DoutPrefixProvider*, const std::string&,
                                      const std::set<std::string>&, Attrs*) {
  return 0;
}

int FakeObject::omap_set_val_by_key(const DoutPrefixProvider*, const std::string&,
                                    bufferlist&, bool, optional_yield) {
  return 0;
}

int FakeObject::chown(User&, const DoutPrefixProvider*, optional_yield) { return 0; }

std::unique_ptr<Object> FakeObject::clone() {
  return std::make_unique<FakeObject>(*this);
}

rgw_obj_key& FakeObject::get_key() { return key_; }

void FakeObject::set_instance(const std::string& i) {
  key_.instance = i;
  obj_.key.instance = i;
}

const std::string& FakeObject::get_instance() const { return key_.instance; }

bool FakeObject::have_instance() { return !key_.instance.empty(); }

void FakeObject::clear_instance() {
  key_.instance.clear();
  obj_.key.instance.clear();
}

void FakeObject::print(std::ostream& out) const {
  out << "FakeObject{" << key_.name << "}";
}

FakeMultipartUpload::FakeMultipartUpload() = default;

FakeMultipartUpload::FakeMultipartUpload(std::string meta, std::string key,
                                         std::string upload_id)
    : meta_(std::move(meta)), key_(std::move(key)), upload_id_(std::move(upload_id)) {}

const std::string& FakeMultipartUpload::get_meta() const { return meta_; }

const std::string& FakeMultipartUpload::get_key() const { return key_; }

const std::string& FakeMultipartUpload::get_upload_id() const { return upload_id_; }

const ACLOwner& FakeMultipartUpload::get_owner() const { return owner_; }

ceph::real_time& FakeMultipartUpload::get_mtime() { return mtime_; }

std::map<uint32_t, std::unique_ptr<MultipartPart>>& FakeMultipartUpload::get_parts() {
  return parts_;
}

const jspan_context& FakeMultipartUpload::get_trace() { return trace_; }

std::unique_ptr<rgw::sal::Object> FakeMultipartUpload::get_meta_obj() {
  rgw_obj_key meta_key(meta_);
  auto obj = std::make_unique<FakeObject>(bucket_key_, meta_key);
  obj->set_bucket(nullptr);
  return obj;
}

int FakeMultipartUpload::init(const DoutPrefixProvider*, optional_yield, ACLOwner& owner,
                              rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) {
  owner_ = owner;
  placement_ = dest_placement;
  attrs_ = attrs;
  return 0;
}

int FakeMultipartUpload::list_parts(const DoutPrefixProvider*, CephContext*, int,
                                    int marker, int* next_marker, bool* truncated,
                                    bool) {
  if (next_marker) {
    *next_marker = marker;
  }
  if (truncated) {
    *truncated = false;
  }
  return 0;
}

int FakeMultipartUpload::abort(const DoutPrefixProvider*, CephContext*) { return 0; }

int FakeMultipartUpload::complete(const DoutPrefixProvider*, optional_yield, CephContext*,
                                  std::map<int, std::string>&, std::list<rgw_obj_index_key>&,
                                  uint64_t& accounted_size, bool& compressed,
                                  RGWCompressionInfo&, off_t& ofs, std::string& tag,
                                  ACLOwner& owner, uint64_t, rgw::sal::Object*) {
  accounted_size = 0;
  compressed = false;
  ofs = 0;
  tag.clear();
  owner = owner_;
  return 0;
}

int FakeMultipartUpload::get_info(const DoutPrefixProvider*, optional_yield,
                                  rgw_placement_rule** rule, rgw::sal::Attrs* attrs) {
  if (rule) {
    *rule = &placement_;
  }
  if (attrs) {
    *attrs = attrs_;
  }
  return 0;
}

std::unique_ptr<Writer> FakeMultipartUpload::get_writer(const DoutPrefixProvider*,
                                                        optional_yield,
                                                        rgw::sal::Object*,
                                                        const rgw_user&,
                                                        const rgw_placement_rule*,
                                                        uint64_t,
                                                        const std::string&) {
  return nullptr;
}

void FakeMultipartUpload::print(std::ostream& out) const {
  out << "FakeMultipartUpload{" << key_ << "/" << upload_id_ << "}";
}

FakeBucket::FakeBucket() = default;

FakeBucket::FakeBucket(const rgw_bucket& key) : key_(key) {
  info_.bucket = key_;
}

std::unique_ptr<Object> FakeBucket::get_object(const rgw_obj_key& key) {
  auto obj = std::make_unique<FakeObject>(key_, key);
  obj->set_bucket(this);
  return obj;
}

int FakeBucket::list(const DoutPrefixProvider*, ListParams&, int, ListResults& results,
                     optional_yield) {
  results.is_truncated = false;
  return 0;
}

Attrs& FakeBucket::get_attrs() { return attrs_; }

int FakeBucket::set_attrs(Attrs a) {
  attrs_ = std::move(a);
  return 0;
}

int FakeBucket::remove_bucket(const DoutPrefixProvider*, bool, bool, req_info*,
                              optional_yield) {
  return 0;
}

int FakeBucket::remove_bucket_bypass_gc(int, bool, optional_yield,
                                        const DoutPrefixProvider*) {
  return 0;
}

RGWAccessControlPolicy& FakeBucket::get_acl() { return acl_; }

int FakeBucket::set_acl(const DoutPrefixProvider*, RGWAccessControlPolicy& acl,
                        optional_yield) {
  acl_ = acl;
  return 0;
}

void FakeBucket::set_owner(rgw::sal::User* owner) { owner_ = owner; }

int FakeBucket::load_bucket(const DoutPrefixProvider*, optional_yield, bool) {
  return 0;
}

int FakeBucket::read_stats(const DoutPrefixProvider*,
                           const bucket_index_layout_generation&, int,
                           std::string* bucket_ver, std::string* master_ver,
                           std::map<RGWObjCategory, RGWStorageStats>& stats,
                           std::string* max_marker, bool* syncstopped) {
  if (bucket_ver) {
    bucket_ver->clear();
  }
  if (master_ver) {
    master_ver->clear();
  }
  if (max_marker) {
    max_marker->clear();
  }
  if (syncstopped) {
    *syncstopped = false;
  }
  stats.clear();
  return 0;
}

int FakeBucket::read_stats_async(const DoutPrefixProvider*,
                                 const bucket_index_layout_generation&, int,
                                 RGWGetBucketStats_CB*) {
  return 0;
}

int FakeBucket::sync_user_stats(const DoutPrefixProvider*, optional_yield) { return 0; }

int FakeBucket::update_container_stats(const DoutPrefixProvider*) { return 0; }

int FakeBucket::check_bucket_shards(const DoutPrefixProvider*) { return 0; }

int FakeBucket::chown(const DoutPrefixProvider*, User& new_user, optional_yield) {
  owner_ = &new_user;
  return 0;
}

int FakeBucket::put_info(const DoutPrefixProvider*, bool, ceph::real_time mtime) {
  modification_time_ = mtime;
  return 0;
}

bool FakeBucket::is_owner(User* user) { return owner_ == user; }

User* FakeBucket::get_owner() { return owner_; }

ACLOwner FakeBucket::get_acl_owner() { return ACLOwner{}; }

int FakeBucket::check_empty(const DoutPrefixProvider*, optional_yield) { return 0; }

int FakeBucket::check_quota(const DoutPrefixProvider*, RGWQuota&, uint64_t,
                            optional_yield, bool) {
  return 0;
}

int FakeBucket::merge_and_store_attrs(const DoutPrefixProvider*, Attrs& new_attrs,
                                      optional_yield) {
  for (const auto& [k, v] : new_attrs) {
    attrs_[k] = v;
  }
  return 0;
}

int FakeBucket::try_refresh_info(const DoutPrefixProvider*, ceph::real_time*) {
  return 0;
}

int FakeBucket::read_usage(const DoutPrefixProvider*, uint64_t, uint64_t, uint32_t,
                           bool* is_truncated, RGWUsageIter&,
                           std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  if (is_truncated) {
    *is_truncated = false;
  }
  usage.clear();
  return 0;
}

int FakeBucket::trim_usage(const DoutPrefixProvider*, uint64_t, uint64_t) { return 0; }

int FakeBucket::remove_objs_from_index(const DoutPrefixProvider*,
                                       std::list<rgw_obj_index_key>&) {
  return 0;
}

int FakeBucket::check_index(const DoutPrefixProvider*,
                            std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
                            std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) {
  existing_stats.clear();
  calculated_stats.clear();
  return 0;
}

int FakeBucket::rebuild_index(const DoutPrefixProvider*) { return 0; }

int FakeBucket::set_tag_timeout(const DoutPrefixProvider*, uint64_t) { return 0; }

int FakeBucket::purge_instance(const DoutPrefixProvider*) { return 0; }

bool FakeBucket::empty() const {
  return key_.name.empty() && key_.bucket_id.empty();
}

const std::string& FakeBucket::get_name() const { return key_.name; }

const std::string& FakeBucket::get_tenant() const { return key_.tenant; }

const std::string& FakeBucket::get_marker() const { return marker_; }

const std::string& FakeBucket::get_bucket_id() const { return key_.bucket_id; }

size_t FakeBucket::get_size() const { return size_; }

size_t FakeBucket::get_size_rounded() const { return size_; }

uint64_t FakeBucket::get_count() const { return count_; }

rgw_placement_rule& FakeBucket::get_placement_rule() { return placement_; }

ceph::real_time& FakeBucket::get_creation_time() { return creation_time_; }

ceph::real_time& FakeBucket::get_modification_time() { return modification_time_; }

obj_version& FakeBucket::get_version() { return version_; }

void FakeBucket::set_version(obj_version& ver) { version_ = ver; }

bool FakeBucket::versioned() { return versioned_; }

bool FakeBucket::versioning_enabled() { return versioning_enabled_; }

std::unique_ptr<Bucket> FakeBucket::clone() {
  return std::make_unique<FakeBucket>(*this);
}

std::unique_ptr<MultipartUpload> FakeBucket::get_multipart_upload(
    const std::string& oid, std::optional<std::string> upload_id, ACLOwner owner,
    ceph::real_time mtime) {
  auto up = std::make_unique<FakeMultipartUpload>(oid, oid,
                                                  upload_id ? *upload_id : std::string{});
  up->set_bucket_key(key_);
  return up;
}

int FakeBucket::list_multiparts(const DoutPrefixProvider*, const std::string&,
                                std::string&, const std::string&, const int&,
                                std::vector<std::unique_ptr<MultipartUpload>>& uploads,
                                std::map<std::string, bool>* common_prefixes,
                                bool* is_truncated) {
  uploads.clear();
  if (common_prefixes) {
    common_prefixes->clear();
  }
  if (is_truncated) {
    *is_truncated = false;
  }
  return 0;
}

int FakeBucket::abort_multiparts(const DoutPrefixProvider*, CephContext*) { return 0; }

int FakeBucket::read_topics(rgw_pubsub_bucket_topics&, RGWObjVersionTracker*,
                            optional_yield, const DoutPrefixProvider*) {
  return 0;
}

int FakeBucket::write_topics(const rgw_pubsub_bucket_topics&, RGWObjVersionTracker*,
                             optional_yield, const DoutPrefixProvider*) {
  return 0;
}

int FakeBucket::remove_topics(RGWObjVersionTracker*, optional_yield,
                              const DoutPrefixProvider*) {
  return 0;
}

rgw_bucket& FakeBucket::get_key() { return key_; }

RGWBucketInfo& FakeBucket::get_info() { return info_; }

void FakeBucket::print(std::ostream& out) const {
  out << "FakeBucket{" << key_.tenant << "/" << key_.name << "}";
}

bool FakeBucket::operator==(const Bucket& b) const {
  return &b == this;
}

bool FakeBucket::operator!=(const Bucket& b) const { return !(*this == b); }

FakeUser::FakeUser() = default;

FakeUser::FakeUser(const rgw_user& id) : id_(id) {
  info_.user_id = id;
}

std::unique_ptr<User> FakeUser::clone() {
  return std::make_unique<FakeUser>(*this);
}

int FakeUser::list_buckets(const DoutPrefixProvider*, const std::string&,
                           const std::string&, uint64_t, bool, BucketList& buckets,
                           optional_yield) {
  buckets.clear();
  return 0;
}

int FakeUser::create_bucket(const DoutPrefixProvider*, const rgw_bucket& b,
                            const std::string&, rgw_placement_rule&,
                            std::string&, const RGWQuotaInfo*,
                            const RGWAccessControlPolicy&, Attrs& attrs,
                            RGWBucketInfo& info, obj_version&, bool, bool,
                            bool* existed, req_info&, std::unique_ptr<Bucket>* bucket,
                            optional_yield) {
  info.bucket = b;
  if (bucket) {
    auto fb = std::make_unique<FakeBucket>(b);
    fb->set_owner(this);
    *bucket = std::move(fb);
  }
  if (existed) {
    *existed = false;
  }
  attrs_ = attrs;
  return 0;
}

std::string& FakeUser::get_display_name() { return display_name_; }

const std::string& FakeUser::get_tenant() { return tenant_; }

void FakeUser::set_tenant(std::string& t) { tenant_ = t; }

const std::string& FakeUser::get_ns() { return ns_; }

void FakeUser::set_ns(std::string& ns) { ns_ = ns; }

void FakeUser::clear_ns() { ns_.clear(); }

const rgw_user& FakeUser::get_id() const { return id_; }

uint32_t FakeUser::get_type() const { return user_type_; }

int32_t FakeUser::get_max_buckets() const { return max_buckets_; }

const RGWUserCaps& FakeUser::get_caps() const { return caps_; }

RGWObjVersionTracker& FakeUser::get_version_tracker() { return version_tracker_; }

Attrs& FakeUser::get_attrs() { return attrs_; }

void FakeUser::set_attrs(Attrs& attrs) { attrs_ = attrs; }

bool FakeUser::empty() const { return id_.empty(); }

int FakeUser::read_attrs(const DoutPrefixProvider*, optional_yield) { return 0; }

int FakeUser::merge_and_store_attrs(const DoutPrefixProvider*, Attrs& new_attrs,
                                    optional_yield) {
  for (const auto& [k, v] : new_attrs) {
    attrs_[k] = v;
  }
  return 0;
}

int FakeUser::read_stats(const DoutPrefixProvider*, optional_yield, RGWStorageStats* stats,
                         ceph::real_time* last_stats_sync,
                         ceph::real_time* last_stats_update) {
  if (stats) {
    *stats = RGWStorageStats{};
  }
  if (last_stats_sync) {
    *last_stats_sync = ceph::real_time{};
  }
  if (last_stats_update) {
    *last_stats_update = ceph::real_time{};
  }
  return 0;
}

int FakeUser::read_stats_async(const DoutPrefixProvider*, RGWGetUserStats_CB*) { return 0; }

int FakeUser::complete_flush_stats(const DoutPrefixProvider*, optional_yield) { return 0; }

int FakeUser::read_usage(const DoutPrefixProvider*, uint64_t, uint64_t, uint32_t,
                         bool* is_truncated, RGWUsageIter& usage_iter,
                         std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  if (is_truncated) {
    *is_truncated = false;
  }
  usage.clear();
  usage_iter = {};
  return 0;
}

int FakeUser::trim_usage(const DoutPrefixProvider*, uint64_t, uint64_t) { return 0; }

int FakeUser::load_user(const DoutPrefixProvider*, optional_yield) { return 0; }

int FakeUser::store_user(const DoutPrefixProvider*, optional_yield, bool,
                         RGWUserInfo*) {
  return 0;
}

int FakeUser::remove_user(const DoutPrefixProvider*, optional_yield) { return 0; }

int FakeUser::verify_mfa(const std::string&, bool* verified,
                         const DoutPrefixProvider*, optional_yield) {
  if (verified) {
    *verified = true;
  }
  return 0;
}

RGWUserInfo& FakeUser::get_info() { return info_; }

void FakeUser::print(std::ostream& out) const {
  out << "FakeUser{" << id_.to_str() << "}";
}

int FakeDriver::initialize(CephContext* cct, const DoutPrefixProvider*) {
  cct_ = cct;
  return 0;
}

const std::string FakeDriver::get_name() const { return name_; }

std::string FakeDriver::get_cluster_id(const DoutPrefixProvider*, optional_yield) {
  return cluster_id_;
}

std::unique_ptr<User> FakeDriver::get_user(const rgw_user& u) {
  return std::make_unique<FakeUser>(u);
}

int FakeDriver::get_user_by_access_key(const DoutPrefixProvider*, const std::string& key,
                                       optional_yield, std::unique_ptr<User>* user) {
  if (user) {
    *user = std::make_unique<FakeUser>(rgw_user(key));
  }
  return 0;
}

int FakeDriver::get_user_by_email(const DoutPrefixProvider*, const std::string& email,
                                  optional_yield, std::unique_ptr<User>* user) {
  if (user) {
    *user = std::make_unique<FakeUser>(rgw_user(email));
  }
  return 0;
}

int FakeDriver::get_user_by_swift(const DoutPrefixProvider*, const std::string& user_str,
                                  optional_yield, std::unique_ptr<User>* user) {
  if (user) {
    *user = std::make_unique<FakeUser>(rgw_user(user_str));
  }
  return 0;
}

std::unique_ptr<Object> FakeDriver::get_object(const rgw_obj_key& k) {
  rgw_bucket bucket;
  return std::make_unique<FakeObject>(bucket, k);
}

int FakeDriver::get_bucket(User* u, const RGWBucketInfo& i,
                           std::unique_ptr<Bucket>* bucket) {
  if (bucket) {
    auto fb = std::make_unique<FakeBucket>(i.bucket);
    fb->set_owner(u);
    *bucket = std::move(fb);
  }
  return 0;
}

int FakeDriver::get_bucket(const DoutPrefixProvider*, User* u, const rgw_bucket& b,
                           std::unique_ptr<Bucket>* bucket, optional_yield) {
  if (bucket) {
    auto fb = std::make_unique<FakeBucket>(b);
    fb->set_owner(u);
    *bucket = std::move(fb);
  }
  return 0;
}

int FakeDriver::get_bucket(const DoutPrefixProvider* dpp, User* u,
                           const std::string& tenant, const std::string& name,
                           std::unique_ptr<Bucket>* bucket, optional_yield y) {
  rgw_bucket b;
  b.tenant = tenant;
  b.name = name;
  return get_bucket(dpp, u, b, bucket, y);
}

bool FakeDriver::is_meta_master() { return false; }

int FakeDriver::forward_request_to_master(const DoutPrefixProvider*, User*, obj_version*,
                                          bufferlist&, JSONParser*, req_info&,
                                          optional_yield) {
  return 0;
}

int FakeDriver::forward_iam_request_to_master(const DoutPrefixProvider*,
                                              const RGWAccessKey&, obj_version*,
                                              bufferlist&, RGWXMLDecoder::XMLParser*,
                                              req_info&, optional_yield) {
  return 0;
}

Zone* FakeDriver::get_zone() { return nullptr; }

std::string FakeDriver::zone_unique_id(uint64_t unique_num) {
  return "zone-" + std::to_string(unique_num);
}

std::string FakeDriver::zone_unique_trans_id(const uint64_t unique_num) {
  return "txn-" + std::to_string(unique_num);
}

int FakeDriver::get_zonegroup(const std::string&, std::unique_ptr<ZoneGroup>*) {
  return 0;
}

int FakeDriver::list_all_zones(const DoutPrefixProvider*,
                               std::list<std::string>& zone_ids) {
  zone_ids.clear();
  return 0;
}

int FakeDriver::cluster_stat(RGWClusterStat& stats) {
  stats = RGWClusterStat{};
  return 0;
}

std::unique_ptr<Lifecycle> FakeDriver::get_lifecycle() { return nullptr; }

std::unique_ptr<Completions> FakeDriver::get_completions() { return nullptr; }

std::unique_ptr<Notification> FakeDriver::get_notification(rgw::sal::Object*,
                                                            rgw::sal::Object*,
                                                            req_state*,
                                                            rgw::notify::EventType,
                                                            optional_yield,
                                                            const std::string*) {
  return nullptr;
}

std::unique_ptr<Notification> FakeDriver::get_notification(
    const DoutPrefixProvider*, rgw::sal::Object*, rgw::sal::Object*,
    rgw::notify::EventType, rgw::sal::Bucket*, std::string&, std::string&, std::string&,
    optional_yield) {
  return nullptr;
}

int FakeDriver::read_topics(const std::string&, rgw_pubsub_topics&, RGWObjVersionTracker*,
                            optional_yield, const DoutPrefixProvider*) {
  return 0;
}

int FakeDriver::write_topics(const std::string&, const rgw_pubsub_topics&,
                             RGWObjVersionTracker*, optional_yield,
                             const DoutPrefixProvider*) {
  return 0;
}

int FakeDriver::remove_topics(const std::string&, RGWObjVersionTracker*, optional_yield,
                              const DoutPrefixProvider*) {
  return 0;
}

RGWLC* FakeDriver::get_rgwlc() { return nullptr; }

RGWCoroutinesManagerRegistry* FakeDriver::get_cr_registry() { return nullptr; }

int FakeDriver::log_usage(const DoutPrefixProvider*,
                          std::map<rgw_user_bucket, RGWUsageBatch>&) {
  return 0;
}

int FakeDriver::log_op(const DoutPrefixProvider*, std::string&, bufferlist&) {
  return 0;
}

int FakeDriver::register_to_service_map(const DoutPrefixProvider*,
                                        const std::string&,
                                        const std::map<std::string, std::string>&) {
  return 0;
}

void FakeDriver::get_quota(RGWQuota& quota) { quota = default_quota_; }

void FakeDriver::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit,
                               RGWRateLimitInfo& user_ratelimit,
                               RGWRateLimitInfo& anon_ratelimit) {
  bucket_ratelimit = bucket_rate_;
  user_ratelimit = user_rate_;
  anon_ratelimit = anon_rate_;
}

int FakeDriver::set_buckets_enabled(const DoutPrefixProvider*,
                                    std::vector<rgw_bucket>&, bool) {
  return 0;
}

uint64_t FakeDriver::get_new_req_id() { return ++req_id_; }

int FakeDriver::get_sync_policy_handler(const DoutPrefixProvider*,
                                        std::optional<rgw_zone_id>,
                                        std::optional<rgw_bucket>,
                                        RGWBucketSyncPolicyHandlerRef* phandler,
                                        optional_yield) {
  if (phandler) {
    phandler->reset();
  }
  return 0;
}

RGWDataSyncStatusManager* FakeDriver::get_data_sync_manager(const rgw_zone_id&) {
  return nullptr;
}

void FakeDriver::wakeup_meta_sync_shards(std::set<int>&) {}

void FakeDriver::wakeup_data_sync_shards(
    const DoutPrefixProvider*, const rgw_zone_id&,
    boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>&) {}

int FakeDriver::clear_usage(const DoutPrefixProvider*) { return 0; }

int FakeDriver::read_all_usage(const DoutPrefixProvider*, uint64_t, uint64_t, uint32_t,
                               bool* is_truncated, RGWUsageIter& usage_iter,
                               std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  if (is_truncated) {
    *is_truncated = false;
  }
  usage_iter = {};
  usage.clear();
  return 0;
}

int FakeDriver::trim_all_usage(const DoutPrefixProvider*, uint64_t, uint64_t) {
  return 0;
}

int FakeDriver::get_config_key_val(std::string, bufferlist* bl) {
  if (bl) {
    bl->clear();
  }
  return 0;
}

int FakeDriver::meta_list_keys_init(const DoutPrefixProvider*, const std::string& section,
                                    const std::string& marker, void** phandle) {
  meta_marker_ = section + marker;
  if (phandle) {
    *phandle = nullptr;
  }
  return 0;
}

int FakeDriver::meta_list_keys_next(const DoutPrefixProvider*, void*, int,
                                    std::list<std::string>& keys, bool* truncated) {
  keys.clear();
  if (truncated) {
    *truncated = false;
  }
  return 0;
}

void FakeDriver::meta_list_keys_complete(void*) {}

std::string FakeDriver::meta_get_marker(void*) { return meta_marker_; }

int FakeDriver::meta_remove(const DoutPrefixProvider*, std::string&, optional_yield) {
  return 0;
}

const RGWSyncModuleInstanceRef& FakeDriver::get_sync_module() { return sync_module_; }

std::string FakeDriver::get_host_id() { return host_id_; }

std::unique_ptr<LuaManager> FakeDriver::get_lua_manager() { return nullptr; }

std::unique_ptr<RGWRole> FakeDriver::get_role(std::string, std::string, std::string,
                                              std::string, std::string,
                                              std::multimap<std::string, std::string>) {
  return nullptr;
}

std::unique_ptr<RGWRole> FakeDriver::get_role(std::string) { return nullptr; }

std::unique_ptr<RGWRole> FakeDriver::get_role(const RGWRoleInfo&) { return nullptr; }

int FakeDriver::get_roles(const DoutPrefixProvider*, optional_yield,
                          const std::string&, const std::string&,
                          std::vector<std::unique_ptr<RGWRole>>&) {
  return 0;
}

std::unique_ptr<RGWOIDCProvider> FakeDriver::get_oidc_provider() { return nullptr; }

int FakeDriver::get_oidc_providers(const DoutPrefixProvider*, const std::string&,
                                   std::vector<std::unique_ptr<RGWOIDCProvider>>&) {
  return 0;
}

std::unique_ptr<Writer> FakeDriver::get_append_writer(const DoutPrefixProvider*,
                                                      optional_yield,
                                                      rgw::sal::Object*,
                                                      const rgw_user&,
                                                      const rgw_placement_rule*,
                                                      const std::string&, uint64_t,
                                                      uint64_t*) {
  return nullptr;
}

std::unique_ptr<Writer> FakeDriver::get_atomic_writer(const DoutPrefixProvider*,
                                                      optional_yield,
                                                      rgw::sal::Object*,
                                                      const rgw_user&,
                                                      const rgw_placement_rule*,
                                                      uint64_t,
                                                      const std::string&) {
  return nullptr;
}

const std::string& FakeDriver::get_compression_type(const rgw_placement_rule&) {
  return compression_type_;
}

bool FakeDriver::valid_placement(const rgw_placement_rule&) { return true; }

void FakeDriver::finalize() {}

CephContext* FakeDriver::ctx() { return cct_; }

void FakeDriver::register_admin_apis(RGWRESTMgr*) {}

} // namespace akamai::fake
