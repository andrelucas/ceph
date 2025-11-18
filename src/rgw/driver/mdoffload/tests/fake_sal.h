// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/**
 * @file fake_sal.h
 * @author André Lucas (alucas@akamai.com)
 * @brief Fake rgw::sal definitions for unit tests.
 * @version 0.1
 * @date 2025-11-17
 *
 * @copyright Copyright (c) 2025 Akamai Inc.
 *
 */

#pragma once

#include <cerrno>
#include <list>
#include <map>
#include <memory>
#include <opentelemetry/trace/span_context.h>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "common/async/yield_context.h"
#include "rgw/rgw_sal.h"
#include "rgw_pubsub.h"
#include "common/tracer.h"

namespace akamai::fake {

using namespace ::rgw::sal;
using rgw::bucket_index_layout_generation;

class FakeBucket;
class FakeObject;
class FakeMultipartUpload;

// Minimal concrete classes that satisfy rgw::sal interfaces for filter unit tests.

class FakeMultipartPart : public MultipartPart {
public:
  FakeMultipartPart() = default;
  FakeMultipartPart(uint32_t num, uint64_t size, std::string etag);

  uint32_t get_num() override;
  uint64_t get_size() override;
  const std::string& get_etag() override;
  ceph::real_time& get_mtime() override;

  void set_num(uint32_t num) { num_ = num; }
  void set_size(uint64_t size) { size_ = size; }
  void set_etag(std::string etag) { etag_ = std::move(etag); }

private:
  uint32_t num_{0};
  uint64_t size_{0};
  std::string etag_;
  ceph::real_time mtime_{};
};

class FakeObject : public Object {
public:
  FakeObject();
  explicit FakeObject(const rgw_bucket& bucket, const rgw_obj_key& key);
  FakeObject(const FakeObject&) = default;
  FakeObject& operator=(const FakeObject&) = default;
  ~FakeObject() override = default;

  int delete_object(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;
  int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
                     bool keep_index_consistent, optional_yield y) override;
  int copy_object(User* user, req_info* info, const rgw_zone_id& source_zone,
                  rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
                  rgw::sal::Bucket* src_bucket, const rgw_placement_rule& dest_placement,
                  ceph::real_time* src_mtime, ceph::real_time* mtime,
                  const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
                  bool high_precision_time, const char* if_match, const char* if_nomatch,
                  AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs, RGWObjCategory category,
                  uint64_t olh_epoch, boost::optional<ceph::real_time> delete_at,
                  std::string* version_id, std::string* tag, std::string* etag,
                  void (*progress_cb)(off_t, void*), void* progress_data,
                  const DoutPrefixProvider* dpp, optional_yield y) override;
  RGWAccessControlPolicy& get_acl() override;
  int set_acl(const RGWAccessControlPolicy& acl) override;
  void set_atomic() override;
  bool is_atomic() override;
  void set_prefetch_data() override;
  bool is_prefetch_data() override;
  void set_compressed() override;
  bool is_compressed() override;
  void invalidate() override;
  bool empty() const override;
  const std::string& get_name() const override;
  int get_obj_state(const DoutPrefixProvider* dpp, RGWObjState** state, optional_yield y,
                    bool follow_olh = true) override;
  int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs,
                    optional_yield y) override;
  int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                    rgw_obj* target_obj = nullptr) override;
  int modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y,
                       const DoutPrefixProvider* dpp) override;
  int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                       optional_yield y) override;
  bool is_expired() override;
  void gen_rand_obj_instance_name() override;
  std::unique_ptr<MPSerializer> get_serializer(const DoutPrefixProvider* dpp,
                                               const std::string& lock_name) override;
  int transition(Bucket* bucket, const rgw_placement_rule& placement_rule,
                 const real_time& mtime, uint64_t olh_epoch, const DoutPrefixProvider* dpp,
                 optional_yield y, uint32_t flags) override;
  int transition_to_cloud(Bucket* bucket, rgw::sal::PlacementTier* tier,
                          rgw_bucket_dir_entry& o, std::set<std::string>& cloud_targets,
                          CephContext* cct, bool update_object, const DoutPrefixProvider* dpp,
                          optional_yield y) override;
  bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;
  int dump_obj_layout(const DoutPrefixProvider* dpp, optional_yield y, Formatter* f) override;
  Attrs& get_attrs() override;
  const Attrs& get_attrs() const override;
  int set_attrs(Attrs a) override;
  bool has_attrs() override;
  ceph::real_time get_mtime() const override;
  uint64_t get_obj_size() const override;
  Bucket* get_bucket() const override;
  void set_bucket(Bucket* b) override;
  std::string get_hash_source() override;
  void set_hash_source(std::string s) override;
  std::string get_oid() const override;
  bool get_delete_marker() override;
  bool get_in_extra_data() override;
  void set_in_extra_data(bool i) override;
  void set_obj_size(uint64_t s) override;
  void set_name(const std::string& n) override;
  void set_key(const rgw_obj_key& k) override;
  rgw_obj get_obj() const override;
  int swift_versioning_restore(bool& restored, const DoutPrefixProvider* dpp) override;
  int swift_versioning_copy(const DoutPrefixProvider* dpp, optional_yield y) override;
  std::unique_ptr<ReadOp> get_read_op() override;
  std::unique_ptr<DeleteOp> get_delete_op() override;
  int omap_get_vals(const DoutPrefixProvider* dpp, const std::string& marker, uint64_t count,
                    std::map<std::string, bufferlist>* m, bool* pmore,
                    optional_yield y) override;
  int omap_get_all(const DoutPrefixProvider* dpp, std::map<std::string, bufferlist>* m,
                   optional_yield y) override;
  int omap_get_vals_by_keys(const DoutPrefixProvider* dpp, const std::string& oid,
                            const std::set<std::string>& keys, Attrs* vals) override;
  int omap_set_val_by_key(const DoutPrefixProvider* dpp, const std::string& key,
                          bufferlist& val, bool must_exist, optional_yield y) override;
  int chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y) override;
  std::unique_ptr<Object> clone() override;
  rgw_obj_key& get_key() override;
  void set_instance(const std::string& i) override;
  const std::string& get_instance() const override;
  bool have_instance() override;
  void clear_instance() override;
  void print(std::ostream& out) const override;

private:
  rgw_bucket bucket_key_{};
  rgw_obj_key key_{};
  Attrs attrs_{};
  RGWAccessControlPolicy acl_{};
  Bucket* bucket_{nullptr};
  bool atomic_{false};
  bool prefetch_{false};
  bool compressed_{false};
  bool delete_marker_{false};
  bool in_extra_data_{false};
  bool has_attrs_{false};
  ceph::real_time mtime_{};
  uint64_t size_{0};
  std::string hash_source_{};
  rgw_obj obj_{};
};

class FakeMultipartUpload : public MultipartUpload {
public:
  FakeMultipartUpload();
  FakeMultipartUpload(std::string meta, std::string key, std::string upload_id);
  FakeMultipartUpload(const FakeMultipartUpload&) = default;
  FakeMultipartUpload& operator=(const FakeMultipartUpload&) = default;
  ~FakeMultipartUpload() override = default;

  const std::string& get_meta() const override;
  const std::string& get_key() const override;
  const std::string& get_upload_id() const override;
  const ACLOwner& get_owner() const override;
  ceph::real_time& get_mtime() override;
  std::map<uint32_t, std::unique_ptr<MultipartPart>>& get_parts() override;
  const jspan_context& get_trace() override;
  std::unique_ptr<rgw::sal::Object> get_meta_obj() override;
  int init(const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner,
           rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) override;
  int list_parts(const DoutPrefixProvider* dpp, CephContext* cct, int num_parts, int marker,
                 int* next_marker, bool* truncated, bool assume_unsorted = false) override;
  int abort(const DoutPrefixProvider* dpp, CephContext* cct) override;
  int complete(const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
               std::map<int, std::string>& part_etags,
               std::list<rgw_obj_index_key>& remove_objs,
               uint64_t& accounted_size, bool& compressed,
               RGWCompressionInfo& cs_info, off_t& ofs,
               std::string& tag, ACLOwner& owner,
               uint64_t olh_epoch, rgw::sal::Object* target_obj) override;
  int get_info(const DoutPrefixProvider* dpp, optional_yield y, rgw_placement_rule** rule,
               rgw::sal::Attrs* attrs = nullptr) override;
  std::unique_ptr<Writer> get_writer(const DoutPrefixProvider* dpp, optional_yield y,
                                     rgw::sal::Object* obj, const rgw_user& owner,
                                     const rgw_placement_rule* ptail_placement_rule,
                                     uint64_t part_num,
                                     const std::string& part_num_str) override;
  void print(std::ostream& out) const override;

  void set_bucket_key(const rgw_bucket& bucket) { bucket_key_ = bucket; }

private:
  std::string meta_{};
  std::string key_{};
  std::string upload_id_{};
  ACLOwner owner_{};
  ceph::real_time mtime_{};
  std::map<uint32_t, std::unique_ptr<MultipartPart>> parts_{};
  // This is initialized to an invalid span context by default. There's no
  // empty constructor for jspan_context.
  jspan_context trace_{opentelemetry::trace::SpanContext::GetInvalid()};
  rgw_bucket bucket_key_{};
  rgw_placement_rule placement_{};
  rgw::sal::Attrs attrs_{};
};

class FakeBucket : public Bucket {
public:
  FakeBucket();
  explicit FakeBucket(const rgw_bucket& key);
  FakeBucket(const FakeBucket&) = default;
  FakeBucket& operator=(const FakeBucket&) = default;
  ~FakeBucket() override = default;

  std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
  int list(const DoutPrefixProvider* dpp, ListParams& params, int, ListResults& results,
           optional_yield y) override;
  Attrs& get_attrs() override;
  int set_attrs(Attrs a) override;
  int remove_bucket(const DoutPrefixProvider* dpp, bool delete_children,
                    bool forward_to_master, req_info* req_info, optional_yield y) override;
  int remove_bucket_bypass_gc(int concurrent_max, bool keep_index_consistent,
                              optional_yield y, const DoutPrefixProvider* dpp) override;
  RGWAccessControlPolicy& get_acl() override;
  int set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl,
              optional_yield y) override;
  void set_owner(rgw::sal::User* owner) override;
  int load_bucket(const DoutPrefixProvider* dpp, optional_yield y, bool get_stats = false) override;
  int read_stats(const DoutPrefixProvider* dpp, const bucket_index_layout_generation& idx_layout,
                 int shard_id, std::string* bucket_ver, std::string* master_ver,
                 std::map<RGWObjCategory, RGWStorageStats>& stats,
                 std::string* max_marker = nullptr,
                 bool* syncstopped = nullptr) override;
  int read_stats_async(const DoutPrefixProvider* dpp,
                       const bucket_index_layout_generation& idx_layout,
                       int shard_id, RGWGetBucketStats_CB* ctx) override;
  int sync_user_stats(const DoutPrefixProvider* dpp, optional_yield y) override;
  int update_container_stats(const DoutPrefixProvider* dpp) override;
  int check_bucket_shards(const DoutPrefixProvider* dpp) override;
  int chown(const DoutPrefixProvider* dpp, User& new_user, optional_yield y) override;
  int put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime) override;
  bool is_owner(User* user) override;
  User* get_owner() override;
  ACLOwner get_acl_owner() override;
  int check_empty(const DoutPrefixProvider* dpp, optional_yield y) override;
  int check_quota(const DoutPrefixProvider* dpp, RGWQuota& quota, uint64_t obj_size,
                  optional_yield y, bool check_size_only = false) override;
  int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs,
                            optional_yield y) override;
  int try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime) override;
  int read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
                 uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
                 std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  int trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                 uint64_t end_epoch) override;
  int remove_objs_from_index(const DoutPrefixProvider* dpp,
                             std::list<rgw_obj_index_key>& objs_to_unlink) override;
  int check_index(const DoutPrefixProvider* dpp,
                  std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
                  std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
  int rebuild_index(const DoutPrefixProvider* dpp) override;
  int set_tag_timeout(const DoutPrefixProvider* dpp, uint64_t timeout) override;
  int purge_instance(const DoutPrefixProvider* dpp) override;
  bool empty() const override;
  const std::string& get_name() const override;
  const std::string& get_tenant() const override;
  const std::string& get_marker() const override;
  const std::string& get_bucket_id() const override;
  size_t get_size() const override;
  size_t get_size_rounded() const override;
  uint64_t get_count() const override;
  rgw_placement_rule& get_placement_rule() override;
  ceph::real_time& get_creation_time() override;
  ceph::real_time& get_modification_time() override;
  obj_version& get_version() override;
  void set_version(obj_version& ver) override;
  bool versioned() override;
  bool versioning_enabled() override;
  std::unique_ptr<Bucket> clone() override;
  std::unique_ptr<MultipartUpload> get_multipart_upload(
      const std::string& oid, std::optional<std::string> upload_id = std::nullopt,
      ACLOwner owner = {}, ceph::real_time mtime = real_clock::now()) override;
  int list_multiparts(const DoutPrefixProvider* dpp, const std::string& prefix,
                      std::string& marker, const std::string& delim, const int& max_uploads,
                      std::vector<std::unique_ptr<MultipartUpload>>& uploads,
                      std::map<std::string, bool>* common_prefixes,
                      bool* is_truncated) override;
  int abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct) override;
  int read_topics(rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* objv_tracker,
                  optional_yield y, const DoutPrefixProvider* dpp) override;
  int write_topics(const rgw_pubsub_bucket_topics& notifications,
                   RGWObjVersionTracker* objv_tracker, optional_yield y,
                   const DoutPrefixProvider* dpp) override;
  int remove_topics(RGWObjVersionTracker* objv_tracker, optional_yield y,
                    const DoutPrefixProvider* dpp) override;
  rgw_bucket& get_key() override;
  RGWBucketInfo& get_info() override;
  void print(std::ostream& out) const override;
  bool operator==(const Bucket& b) const override;
  bool operator!=(const Bucket& b) const override;

private:
  rgw_bucket key_{};
  RGWBucketInfo info_{};
  Attrs attrs_{};
  RGWAccessControlPolicy acl_{};
  User* owner_{nullptr};
  ceph::real_time creation_time_{};
  ceph::real_time modification_time_{};
  obj_version version_{};
  rgw_placement_rule placement_{};
  size_t size_{0};
  uint64_t count_{0};
  std::string marker_{};
  std::string bucket_id_{};
  bool versioned_{false};
  bool versioning_enabled_{false};
};

class FakeUser : public User {
public:
  FakeUser();
  explicit FakeUser(const rgw_user& id);
  FakeUser(const FakeUser&) = default;
  FakeUser& operator=(const FakeUser&) = default;
  ~FakeUser() override = default;

  std::unique_ptr<User> clone() override;
  int list_buckets(const DoutPrefixProvider* dpp, const std::string& marker,
                   const std::string& end_marker, uint64_t max, bool need_stats,
                   BucketList& buckets, optional_yield y) override;
  int create_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                    const std::string& zonegroup_id, rgw_placement_rule& placement_rule,
                    std::string& swift_ver_location, const RGWQuotaInfo* pquota_info,
                    const RGWAccessControlPolicy& policy, Attrs& attrs,
                    RGWBucketInfo& info, obj_version& ep_objv, bool exclusive,
                    bool obj_lock_enabled, bool* existed, req_info& req_info,
                    std::unique_ptr<Bucket>* bucket, optional_yield y) override;
  std::string& get_display_name() override;
  const std::string& get_tenant() override;
  void set_tenant(std::string& t) override;
  const std::string& get_ns() override;
  void set_ns(std::string& ns) override;
  void clear_ns() override;
  const rgw_user& get_id() const override;
  uint32_t get_type() const override;
  int32_t get_max_buckets() const override;
  const RGWUserCaps& get_caps() const override;
  RGWObjVersionTracker& get_version_tracker() override;
  Attrs& get_attrs() override;
  void set_attrs(Attrs& attrs) override;
  bool empty() const override;
  int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
  int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs,
                            optional_yield y) override;
  int read_stats(const DoutPrefixProvider* dpp, optional_yield y, RGWStorageStats* stats,
                 ceph::real_time* last_stats_sync, ceph::real_time* last_stats_update) override;
  int read_stats_async(const DoutPrefixProvider* dpp, RGWGetUserStats_CB* cb) override;
  int complete_flush_stats(const DoutPrefixProvider* dpp, optional_yield y) override;
  int read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
                 uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
                 std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  int trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                 uint64_t end_epoch) override;
  int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
  int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive,
                 RGWUserInfo* old_info = nullptr) override;
  int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;
  int verify_mfa(const std::string& mfa_str, bool* verified, const DoutPrefixProvider* dpp,
                 optional_yield y) override;
  RGWUserInfo& get_info() override;
  void print(std::ostream& out) const override;

private:
  rgw_user id_{};
  std::string display_name_{};
  std::string tenant_{};
  std::string ns_{};
  Attrs attrs_{};
  RGWUserInfo info_{};
  RGWObjVersionTracker version_tracker_{};
  RGWUserCaps caps_{};
  uint32_t user_type_{0};
  int32_t max_buckets_{1000};
};

class FakeDriver : public Driver {
public:
  FakeDriver() = default;
  ~FakeDriver() override = default;

  int initialize(CephContext* cct, const DoutPrefixProvider* dpp) override;
  const std::string get_name() const override;
  std::string get_cluster_id(const DoutPrefixProvider* dpp, optional_yield y) override;
  std::unique_ptr<User> get_user(const rgw_user& u) override;
  int get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key,
                             optional_yield y, std::unique_ptr<User>* user) override;
  int get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email,
                        optional_yield y, std::unique_ptr<User>* user) override;
  int get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str,
                        optional_yield y, std::unique_ptr<User>* user) override;
  std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) override;
  int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b,
                 std::unique_ptr<Bucket>* bucket, optional_yield y) override;
  int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant,
                 const std::string& name, std::unique_ptr<Bucket>* bucket,
                 optional_yield y) override;
  bool is_meta_master() override;
  int forward_request_to_master(const DoutPrefixProvider* dpp, User* user, obj_version* objv,
                                bufferlist& in_data, JSONParser* jp, req_info& info,
                                optional_yield y) override;
  int forward_iam_request_to_master(const DoutPrefixProvider* dpp, const RGWAccessKey& key,
                                    obj_version* objv, bufferlist& in_data,
                                    RGWXMLDecoder::XMLParser* parser, req_info& info,
                                    optional_yield y) override;
  Zone* get_zone() override;
  std::string zone_unique_id(uint64_t unique_num) override;
  std::string zone_unique_trans_id(const uint64_t unique_num) override;
  int get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup) override;
  int list_all_zones(const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids) override;
  int cluster_stat(RGWClusterStat& stats) override;
  std::unique_ptr<Lifecycle> get_lifecycle() override;
  std::unique_ptr<Completions> get_completions() override;
  std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj,
      rgw::sal::Object* src_obj, req_state* s, rgw::notify::EventType event_type,
      optional_yield y, const std::string* object_name) override;
  std::unique_ptr<Notification> get_notification(const DoutPrefixProvider* dpp,
      rgw::sal::Object* obj, rgw::sal::Object* src_obj, rgw::notify::EventType event_type,
      rgw::sal::Bucket* bucket, std::string& user_id, std::string& user_tenant,
      std::string& req_id, optional_yield y) override;
  int read_topics(const std::string& tenant, rgw_pubsub_topics& topics,
                  RGWObjVersionTracker* objv_tracker, optional_yield y,
                  const DoutPrefixProvider* dpp) override;
  int write_topics(const std::string& tenant, const rgw_pubsub_topics& topics,
                   RGWObjVersionTracker* objv_tracker, optional_yield y,
                   const DoutPrefixProvider* dpp) override;
  int remove_topics(const std::string& tenant, RGWObjVersionTracker* objv_tracker,
                    optional_yield y, const DoutPrefixProvider* dpp) override;
  RGWLC* get_rgwlc() override;
  RGWCoroutinesManagerRegistry* get_cr_registry() override;
  int log_usage(const DoutPrefixProvider* dpp,
                std::map<rgw_user_bucket, RGWUsageBatch>& usage_info) override;
  int log_op(const DoutPrefixProvider* dpp, std::string& oid, bufferlist& bl) override;
  int register_to_service_map(const DoutPrefixProvider* dpp, const std::string& daemon_type,
                              const std::map<std::string, std::string>& meta) override;
  void get_quota(RGWQuota& quota) override;
  void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit,
                     RGWRateLimitInfo& anon_ratelimit) override;
  int set_buckets_enabled(const DoutPrefixProvider* dpp, std::vector<rgw_bucket>& buckets,
                          bool enabled) override;
  uint64_t get_new_req_id() override;
  int get_sync_policy_handler(const DoutPrefixProvider* dpp,
                              std::optional<rgw_zone_id> zone,
                              std::optional<rgw_bucket> bucket,
                              RGWBucketSyncPolicyHandlerRef* phandler,
                              optional_yield y) override;
  RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override;
  void wakeup_meta_sync_shards(std::set<int>& shard_ids) override;
  void wakeup_data_sync_shards(const DoutPrefixProvider* dpp,
                               const rgw_zone_id& source_zone,
                               boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids) override;
  int clear_usage(const DoutPrefixProvider* dpp) override;
  int read_all_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
                     uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
                     std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  int trim_all_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                     uint64_t end_epoch) override;
  int get_config_key_val(std::string name, bufferlist* bl) override;
  int meta_list_keys_init(const DoutPrefixProvider* dpp, const std::string& section,
                          const std::string& marker, void** phandle) override;
  int meta_list_keys_next(const DoutPrefixProvider* dpp, void* handle, int max,
                          std::list<std::string>& keys, bool* truncated) override;
  void meta_list_keys_complete(void* handle) override;
  std::string meta_get_marker(void* handle) override;
  int meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key,
                  optional_yield y) override;
  const RGWSyncModuleInstanceRef& get_sync_module() override;
  std::string get_host_id() override;
  std::unique_ptr<LuaManager> get_lua_manager() override;
  std::unique_ptr<RGWRole> get_role(std::string name, std::string tenant,
                                    std::string path, std::string trust_policy,
                                    std::string max_session_duration_str,
                                    std::multimap<std::string, std::string> tags) override;
  std::unique_ptr<RGWRole> get_role(std::string id) override;
  std::unique_ptr<RGWRole> get_role(const RGWRoleInfo& info) override;
  int get_roles(const DoutPrefixProvider* dpp, optional_yield y,
                const std::string& path_prefix, const std::string& tenant,
                std::vector<std::unique_ptr<RGWRole>>& roles) override;
  std::unique_ptr<RGWOIDCProvider> get_oidc_provider() override;
  int get_oidc_providers(const DoutPrefixProvider* dpp, const std::string& tenant,
                         std::vector<std::unique_ptr<RGWOIDCProvider>>& providers) override;
  std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider* dpp, optional_yield y,
                                            rgw::sal::Object* obj, const rgw_user& owner,
                                            const rgw_placement_rule* ptail_placement_rule,
                                            const std::string& unique_tag, uint64_t position,
                                            uint64_t* cur_accounted_size) override;
  std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider* dpp, optional_yield y,
                                            rgw::sal::Object* obj, const rgw_user& owner,
                                            const rgw_placement_rule* ptail_placement_rule,
                                            uint64_t olh_epoch, const std::string& unique_tag) override;
  const std::string& get_compression_type(const rgw_placement_rule& rule) override;
  bool valid_placement(const rgw_placement_rule& rule) override;
  void finalize() override;
  CephContext* ctx() override;
  void register_admin_apis(RGWRESTMgr* mgr) override;

private:
  CephContext* cct_{nullptr};
  std::string name_{"fake-sal"};
  std::string cluster_id_{"fake-cluster"};
  std::string host_id_{"fake-host"};
  std::string compression_type_{"none"};
  uint64_t req_id_{0};
  RGWQuota default_quota_{};
  RGWRateLimitInfo bucket_rate_{};
  RGWRateLimitInfo user_rate_{};
  RGWRateLimitInfo anon_rate_{};
  RGWSyncModuleInstanceRef sync_module_{};
  std::string meta_marker_{};
};

// Utility functions.

/**
 * @brief Return a stable UUID for a given bucket name.
 *
 * This function generates a stable UUID based on the provided bucket name.
 *
 * @param bucket_name
 * @return std::string The generated stable UUID.
 */
std::string stable_uuid_for_bucket_name(const std::string& bucket_name);

} // namespace akamai::fake
