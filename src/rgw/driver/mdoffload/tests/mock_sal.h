// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/**
 * @file mock_sal.h
 * @brief Google Mock classes for rgw::sal interfaces used in tests
 *
 * @details This file provides mock implementations of rgw::sal interfaces
 * using Google Mock. These mocks can be used in unit tests to simulate the
 * behavior of storage abstraction layer components without requiring a full
 * backend.
 *
 * Generated using Copilot in VSCode (GPT-5) with the prompt: "in a separate
 * header file mock_sal.h in src/rgw/driver/mdoffload/tests, create google
 * mock classes for rgw::sal::Driver and other pure virtual classes in
 * src/rgw/rgw_sal.h", with further prompts and manual tweaks to fix
 * compilation errors.
 *
 * Most generation errors were due to Google Mock's limitations on the number
 * of arguments to mocked methods. To work around this, helper structs were
 * introduced to group related parameters together.
 *
 * Further errors were due to incomplete types. To resolve these, necessary
 * headers were included (rgw_pubsub.h).
 *
 */

#pragma once

#include <gmock/gmock.h>

#include "common/async/yield_context.h"
#include "rgw/rgw_sal.h"

// Needed to complete a type so the mocked classes can call sizeof().
#include "rgw_pubsub.h"

namespace rgw { namespace sal {

// Minimal set of mocks commonly needed by filter driver tests. Extend as needed.

// Helper to work around Google Mock argument count limitations
struct CreateBucketParams {
  const DoutPrefixProvider* dpp;
  const rgw_bucket* b;
  std::string zonegroup_id;
  rgw_placement_rule* placement_rule;
  std::string* swift_ver_location;
  const RGWQuotaInfo* pquota_info;
  const RGWAccessControlPolicy* policy;
  Attrs* attrs;
  RGWBucketInfo* info;
  obj_version* ep_objv;
  bool exclusive;
  bool obj_lock_enabled;
  bool* existed;
  req_info* req_info_p;
  std::unique_ptr<Bucket>* bucket;
  optional_yield y;
};

// Helper to work around Google Mock argument count limitations
struct CopyObjectParams {
  User* user;
  req_info* info;
  const rgw_zone_id* source_zone;
  rgw::sal::Object* dest_object;
  rgw::sal::Bucket* dest_bucket;
  rgw::sal::Bucket* src_bucket;
  const rgw_placement_rule* dest_placement;
  ceph::real_time* src_mtime;
  ceph::real_time* mtime;
  const ceph::real_time* mod_ptr;
  const ceph::real_time* unmod_ptr;
  bool high_precision_time;
  const char* if_match;
  const char* if_nomatch;
  AttrsMod attrs_mod;
  bool copy_if_newer;
  Attrs* attrs;
  RGWObjCategory category;
  uint64_t olh_epoch;
  boost::optional<ceph::real_time> delete_at;
  std::string* version_id;
  std::string* tag;
  std::string* etag;
  void (*progress_cb)(off_t, void *);
  void* progress_data;
  const DoutPrefixProvider* dpp;
  optional_yield y;
};

// Helper to work around Google Mock argument count limitations
struct MultipartCompleteParams {
  const DoutPrefixProvider* dpp;
  optional_yield y;
  CephContext* cct;
  std::map<int, std::string>* part_etags;
  std::list<rgw_obj_index_key>* remove_objs;
  uint64_t* accounted_size;
  bool* compressed;
  RGWCompressionInfo* cs_info;
  off_t* ofs;
  std::string* tag;
  ACLOwner* owner;
  uint64_t olh_epoch;
  rgw::sal::Object* target_obj;
};

class MockDriver : public Driver {
public:
  MOCK_METHOD(int, initialize, (CephContext *cct, const DoutPrefixProvider *dpp), (override));
//   MOCK_METHOD(const std::string, get_name, (), (const, override));
  const std::string get_name() const override {
    static const std::string name = "mockdriver";
    return name;
  }
  MOCK_METHOD(std::string, get_cluster_id, (const DoutPrefixProvider* dpp, optional_yield y), (override));
  MOCK_METHOD(std::unique_ptr<User>, get_user, (const rgw_user& u), (override));
  MOCK_METHOD(int, get_user_by_access_key, (const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user), (override));
  MOCK_METHOD(int, get_user_by_email, (const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user), (override));
  MOCK_METHOD(int, get_user_by_swift, (const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user), (override));
  MOCK_METHOD(std::unique_ptr<Object>, get_object, (const rgw_obj_key& k), (override));
  MOCK_METHOD(int, get_bucket, (User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket), (override));
  MOCK_METHOD(int, get_bucket, (const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y), (override));
  MOCK_METHOD(int, get_bucket, (const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y), (override));
  MOCK_METHOD(bool, is_meta_master, (), (override));
  MOCK_METHOD(int, forward_request_to_master, (const DoutPrefixProvider *dpp, User* user, obj_version* objv, bufferlist& in_data, JSONParser* jp, req_info& info, optional_yield y), (override));
  MOCK_METHOD(int, forward_iam_request_to_master, (const DoutPrefixProvider *dpp, const RGWAccessKey& key, obj_version* objv, bufferlist& in_data, RGWXMLDecoder::XMLParser* parser, req_info& info, optional_yield y), (override));
  MOCK_METHOD(Zone*, get_zone, (), (override));
  MOCK_METHOD(std::string, zone_unique_id, (uint64_t unique_num), (override));
  MOCK_METHOD(std::string, zone_unique_trans_id, (const uint64_t unique_num), (override));
  MOCK_METHOD(int, get_zonegroup, (const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup), (override));
  MOCK_METHOD(int, list_all_zones, (const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids), (override));
  MOCK_METHOD(int, cluster_stat, (RGWClusterStat& stats), (override));
  MOCK_METHOD(std::unique_ptr<Lifecycle>, get_lifecycle, (), (override));
  MOCK_METHOD(std::unique_ptr<Completions>, get_completions, (), (override));
  MOCK_METHOD(std::unique_ptr<Notification>, get_notification, (rgw::sal::Object* obj, rgw::sal::Object* src_obj, req_state* s, rgw::notify::EventType event_type, optional_yield y, const std::string* object_name), (override));
  MOCK_METHOD(std::unique_ptr<Notification>, get_notification, (const DoutPrefixProvider* dpp, rgw::sal::Object* obj, rgw::sal::Object* src_obj, rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant, std::string& _req_id, optional_yield y), (override));
  MOCK_METHOD(int, read_topics, (const std::string& tenant, rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker, optional_yield y, const DoutPrefixProvider *dpp), (override));
  MOCK_METHOD(int, write_topics, (const std::string& tenant, const rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker, optional_yield y, const DoutPrefixProvider *dpp), (override));
  MOCK_METHOD(int, remove_topics, (const std::string& tenant, RGWObjVersionTracker* objv_tracker, optional_yield y, const DoutPrefixProvider *dpp), (override));
  MOCK_METHOD(RGWLC*, get_rgwlc, (), (override));
  MOCK_METHOD(RGWCoroutinesManagerRegistry*, get_cr_registry, (), (override));
  MOCK_METHOD(int, log_usage, (const DoutPrefixProvider *dpp, (std::map<rgw_user_bucket, RGWUsageBatch>)& usage_info), (override));
  MOCK_METHOD(int, log_op, (const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl), (override));
  MOCK_METHOD(int, register_to_service_map, (const DoutPrefixProvider *dpp, const std::string& daemon_type, (const std::map<std::string, std::string>)& meta), (override));
  MOCK_METHOD(void, get_quota, (RGWQuota& quota), (override));
  MOCK_METHOD(void, get_ratelimit, (RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit), (override));
  MOCK_METHOD(int, set_buckets_enabled, (const DoutPrefixProvider* dpp, std::vector<rgw_bucket>& buckets, bool enabled), (override));
  MOCK_METHOD(uint64_t, get_new_req_id, (), (override));
  MOCK_METHOD(int, get_sync_policy_handler, (const DoutPrefixProvider* dpp, std::optional<rgw_zone_id> zone, std::optional<rgw_bucket> bucket, RGWBucketSyncPolicyHandlerRef* phandler, optional_yield y), (override));
  MOCK_METHOD(RGWDataSyncStatusManager*, get_data_sync_manager, (const rgw_zone_id& source_zone), (override));
  MOCK_METHOD(void, wakeup_meta_sync_shards, (std::set<int>& shard_ids), (override));
  MOCK_METHOD(void, wakeup_data_sync_shards, (const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, (boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>)& shard_ids), (override));
  MOCK_METHOD(int, clear_usage, (const DoutPrefixProvider *dpp), (override));
  MOCK_METHOD(int, read_all_usage, (const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter, (std::map<rgw_user_bucket, rgw_usage_log_entry>)& usage), (override));
  MOCK_METHOD(int, trim_all_usage, (const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch), (override));
  MOCK_METHOD(int, get_config_key_val, (std::string name, bufferlist* bl), (override));
  MOCK_METHOD(int, meta_list_keys_init, (const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle), (override));
  MOCK_METHOD(int, meta_list_keys_next, (const DoutPrefixProvider *dpp, void* handle, int max, std::list<std::string>& keys, bool* truncated), (override));
  MOCK_METHOD(void, meta_list_keys_complete, (void* handle), (override));
  MOCK_METHOD(std::string, meta_get_marker, (void* handle), (override));
  MOCK_METHOD(int, meta_remove, (const DoutPrefixProvider* dpp, std::string& metadata_key, optional_yield y), (override));
  MOCK_METHOD(const RGWSyncModuleInstanceRef&, get_sync_module, (), (override));
  MOCK_METHOD(std::string, get_host_id, (), (override));
  MOCK_METHOD(std::unique_ptr<LuaManager>, get_lua_manager, (), (override));
  MOCK_METHOD(std::unique_ptr<RGWRole>, get_role, (std::string name, std::string tenant, std::string path, std::string trust_policy, std::string max_session_duration_str, (std::multimap<std::string,std::string>) tags), (override));
  MOCK_METHOD(std::unique_ptr<RGWRole>, get_role, (std::string id), (override));
  MOCK_METHOD(std::unique_ptr<RGWRole>, get_role, (const RGWRoleInfo& info), (override));
  MOCK_METHOD(int, get_roles, (const DoutPrefixProvider *dpp, optional_yield y, const std::string& path_prefix, const std::string& tenant, std::vector<std::unique_ptr<RGWRole>>& roles), (override));
  MOCK_METHOD(std::unique_ptr<RGWOIDCProvider>, get_oidc_provider, (), (override));
  MOCK_METHOD(int, get_oidc_providers, (const DoutPrefixProvider *dpp, const std::string& tenant, std::vector<std::unique_ptr<RGWOIDCProvider>>& providers), (override));
  MOCK_METHOD(std::unique_ptr<Writer>, get_append_writer, (const DoutPrefixProvider *dpp, optional_yield y, rgw::sal::Object* obj, const rgw_user& owner, const rgw_placement_rule *ptail_placement_rule, const std::string& unique_tag, uint64_t position, uint64_t *cur_accounted_size), (override));
  MOCK_METHOD(std::unique_ptr<Writer>, get_atomic_writer, (const DoutPrefixProvider *dpp, optional_yield y, rgw::sal::Object* obj, const rgw_user& owner, const rgw_placement_rule *ptail_placement_rule, uint64_t olh_epoch, const std::string& unique_tag), (override));
  MOCK_METHOD(const std::string&, get_compression_type, (const rgw_placement_rule& rule), (override));
  MOCK_METHOD(bool, valid_placement, (const rgw_placement_rule& rule), (override));
  MOCK_METHOD(void, finalize, (), (override));
  MOCK_METHOD(CephContext*, ctx, (), (override));
  MOCK_METHOD(void, register_admin_apis, (RGWRESTMgr* mgr), (override));
};

class MockUser : public User {
public:
  MOCK_METHOD(std::unique_ptr<User>, clone, (), (override));
  MOCK_METHOD(int, list_buckets, (const DoutPrefixProvider* dpp, const std::string& marker, const std::string& end_marker, uint64_t max, bool need_stats, BucketList& buckets, optional_yield y), (override));
  // Forwarding override to a mockable helper with a single params object to avoid exceeding gMock arg limits
  int create_bucket(const DoutPrefixProvider* dpp,
                    const rgw_bucket& b,
                    const std::string& zonegroup_id,
                    rgw_placement_rule& placement_rule,
                    std::string& swift_ver_location,
                    const RGWQuotaInfo* pquota_info,
                    const RGWAccessControlPolicy& policy,
                    Attrs& attrs,
                    RGWBucketInfo& info,
                    obj_version& ep_objv,
                    bool exclusive,
                    bool obj_lock_enabled,
                    bool* existed,
                    req_info& req_info,
                    std::unique_ptr<Bucket>* bucket,
                    optional_yield y) override {
    CreateBucketParams p{dpp, &b, zonegroup_id, &placement_rule, &swift_ver_location,
                         pquota_info, &policy, &attrs, &info, &ep_objv, exclusive,
                         obj_lock_enabled, existed, &req_info, bucket, y};
    return create_bucket_cb(p);
  }
  MOCK_METHOD(int, create_bucket_cb, (const CreateBucketParams&), ());
  MOCK_METHOD(std::string&, get_display_name, (), (override));
  MOCK_METHOD(const std::string&, get_tenant, (), (override));
  MOCK_METHOD(void, set_tenant, (std::string& _t), (override));
  MOCK_METHOD(const std::string&, get_ns, (), (override));
  MOCK_METHOD(void, set_ns, (std::string& _ns), (override));
  MOCK_METHOD(void, clear_ns, (), (override));
  MOCK_METHOD(const rgw_user&, get_id, (), (const, override));
  MOCK_METHOD(uint32_t, get_type, (), (const, override));
  MOCK_METHOD(int32_t, get_max_buckets, (), (const, override));
  MOCK_METHOD(const RGWUserCaps&, get_caps, (), (const, override));
  MOCK_METHOD(RGWObjVersionTracker&, get_version_tracker, (), (override));
  MOCK_METHOD(Attrs&, get_attrs, (), (override));
  MOCK_METHOD(void, set_attrs, (Attrs& _attrs), (override));
  MOCK_METHOD(bool, empty, (), (const, override));
  MOCK_METHOD(int, read_attrs, (const DoutPrefixProvider* dpp, optional_yield y), (override));
  MOCK_METHOD(int, merge_and_store_attrs, (const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y), (override));
  MOCK_METHOD(int, read_stats, (const DoutPrefixProvider *dpp, optional_yield y, RGWStorageStats* stats, ceph::real_time* last_stats_sync, ceph::real_time* last_stats_update), (override));
  MOCK_METHOD(int, read_stats_async, (const DoutPrefixProvider *dpp, RGWGetUserStats_CB* cb), (override));
  MOCK_METHOD(int, complete_flush_stats, (const DoutPrefixProvider *dpp, optional_yield y), (override));
  MOCK_METHOD(int, read_usage, (const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter, (std::map<rgw_user_bucket, rgw_usage_log_entry>)& usage), (override));
  MOCK_METHOD(int, trim_usage, (const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch), (override));
  MOCK_METHOD(int, load_user, (const DoutPrefixProvider* dpp, optional_yield y), (override));
  MOCK_METHOD(int, store_user, (const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info), (override));
  MOCK_METHOD(int, remove_user, (const DoutPrefixProvider* dpp, optional_yield y), (override));
  MOCK_METHOD(int, verify_mfa, (const std::string& mfa_str, bool* verified, const DoutPrefixProvider* dpp, optional_yield y), (override));
  MOCK_METHOD(RGWUserInfo&, get_info, (), (override));
  MOCK_METHOD(void, print, (std::ostream& out), (const, override));
};

class MockBucket : public Bucket {
public:
  MOCK_METHOD(std::unique_ptr<Object>, get_object, (const rgw_obj_key& key), (override));
  MOCK_METHOD(int, list, (const DoutPrefixProvider* dpp, ListParams&, int, ListResults&, optional_yield y), (override));
  MOCK_METHOD(Attrs&, get_attrs, (), (override));
  MOCK_METHOD(int, set_attrs, (Attrs a), (override));
  MOCK_METHOD(int, remove_bucket, (const DoutPrefixProvider* dpp, bool delete_children, bool forward_to_master, req_info* req_info, optional_yield y), (override));
  MOCK_METHOD(int, remove_bucket_bypass_gc, (int concurrent_max, bool keep_index_consistent, optional_yield y, const DoutPrefixProvider *dpp), (override));
  MOCK_METHOD(RGWAccessControlPolicy&, get_acl, (), (override));
  MOCK_METHOD(int, set_acl, (const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl, optional_yield y), (override));
  MOCK_METHOD(void, set_owner, (rgw::sal::User* _owner), (override));
  MOCK_METHOD(int, load_bucket, (const DoutPrefixProvider* dpp, optional_yield y, bool get_stats), (override));
  MOCK_METHOD(int, read_stats, (const DoutPrefixProvider *dpp, const bucket_index_layout_generation& idx_layout, int shard_id, std::string* bucket_ver, std::string* master_ver, (std::map<RGWObjCategory, RGWStorageStats>)& stats, std::string* max_marker, bool* syncstopped), (override));
  MOCK_METHOD(int, read_stats_async, (const DoutPrefixProvider *dpp, const bucket_index_layout_generation& idx_layout, int shard_id, RGWGetBucketStats_CB* ctx), (override));
  MOCK_METHOD(int, sync_user_stats, (const DoutPrefixProvider *dpp, optional_yield y), (override));
  MOCK_METHOD(int, update_container_stats, (const DoutPrefixProvider* dpp), (override));
  MOCK_METHOD(int, check_bucket_shards, (const DoutPrefixProvider* dpp), (override));
  MOCK_METHOD(int, chown, (const DoutPrefixProvider* dpp, User& new_user, optional_yield y), (override));
  MOCK_METHOD(int, put_info, (const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime), (override));
  MOCK_METHOD(bool, is_owner, (User* user), (override));
  MOCK_METHOD(User*, get_owner, (), (override));
  MOCK_METHOD(ACLOwner, get_acl_owner, (), (override));
  MOCK_METHOD(int, check_empty, (const DoutPrefixProvider* dpp, optional_yield y), (override));
  MOCK_METHOD(int, check_quota, (const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size, optional_yield y, bool check_size_only), (override));
  MOCK_METHOD(int, merge_and_store_attrs, (const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y), (override));
  MOCK_METHOD(int, try_refresh_info, (const DoutPrefixProvider* dpp, ceph::real_time* pmtime), (override));
  MOCK_METHOD(int, read_usage, (const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter, (std::map<rgw_user_bucket, rgw_usage_log_entry>)& usage), (override));
  MOCK_METHOD(int, trim_usage, (const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch), (override));
  MOCK_METHOD(int, remove_objs_from_index, (const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink), (override));
  MOCK_METHOD(int, check_index, (const DoutPrefixProvider *dpp, (std::map<RGWObjCategory, RGWStorageStats>)& existing_stats, (std::map<RGWObjCategory, RGWStorageStats>)& calculated_stats), (override));
  MOCK_METHOD(int, rebuild_index, (const DoutPrefixProvider *dpp), (override));
  MOCK_METHOD(int, set_tag_timeout, (const DoutPrefixProvider *dpp, uint64_t timeout), (override));
  MOCK_METHOD(int, purge_instance, (const DoutPrefixProvider* dpp), (override));
  MOCK_METHOD(bool, empty, (), (const, override));
  MOCK_METHOD(const std::string&, get_name, (), (const, override));
  MOCK_METHOD(const std::string&, get_tenant, (), (const, override));
  MOCK_METHOD(const std::string&, get_marker, (), (const, override));
  MOCK_METHOD(const std::string&, get_bucket_id, (), (const, override));
  MOCK_METHOD(size_t, get_size, (), (const, override));
  MOCK_METHOD(size_t, get_size_rounded, (), (const, override));
  MOCK_METHOD(uint64_t, get_count, (), (const, override));
  MOCK_METHOD(rgw_placement_rule&, get_placement_rule, (), (override));
  MOCK_METHOD(ceph::real_time&, get_creation_time, (), (override));
  MOCK_METHOD(ceph::real_time&, get_modification_time, (), (override));
  MOCK_METHOD(obj_version&, get_version, (), (override));
  MOCK_METHOD(void, set_version, (obj_version &ver), (override));
  MOCK_METHOD(bool, versioned, (), (override));
  MOCK_METHOD(bool, versioning_enabled, (), (override));
  MOCK_METHOD(std::unique_ptr<Bucket>, clone, (), (override));
  MOCK_METHOD(std::unique_ptr<MultipartUpload>, get_multipart_upload, (const std::string& oid, std::optional<std::string> upload_id, ACLOwner owner, ceph::real_time mtime), (override));
  MOCK_METHOD(int, list_multiparts, (const DoutPrefixProvider *dpp, const std::string& prefix, std::string& marker, const std::string& delim, const int& max_uploads, std::vector<std::unique_ptr<MultipartUpload>>& uploads, (std::map<std::string, bool>) *common_prefixes, bool *is_truncated), (override));
  MOCK_METHOD(int, abort_multiparts, (const DoutPrefixProvider* dpp, CephContext* cct), (override));
  MOCK_METHOD(int, read_topics, (rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* objv_tracker, optional_yield y, const DoutPrefixProvider *dpp), (override));
  MOCK_METHOD(int, write_topics, (const rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* objv_tracker, optional_yield y, const DoutPrefixProvider *dpp), (override));
  MOCK_METHOD(int, remove_topics, (RGWObjVersionTracker* objv_tracker, optional_yield y, const DoutPrefixProvider *dpp), (override));
  MOCK_METHOD(rgw_bucket&, get_key, (), (override));
  MOCK_METHOD(RGWBucketInfo&, get_info, (), (override));
  MOCK_METHOD(void, print, (std::ostream& out), (const, override));
//   MOCK_METHOD(bool, operator==, (const Bucket& b), (const, override));
//   MOCK_METHOD(bool, operator!=, (const Bucket& b), (const, override));
};

class MockObject : public Object {
public:
  MOCK_METHOD(int, delete_object, (const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags), (override));
  MOCK_METHOD(int, delete_obj_aio, (const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio, bool keep_index_consistent, optional_yield y), (override));
  // Forwarding override to a mockable helper with a single params object to avoid exceeding gMock arg limits
  int copy_object(User* user,
                  req_info* info,
                  const rgw_zone_id& source_zone,
                  rgw::sal::Object* dest_object,
                  rgw::sal::Bucket* dest_bucket,
                  rgw::sal::Bucket* src_bucket,
                  const rgw_placement_rule& dest_placement,
                  ceph::real_time* src_mtime,
                  ceph::real_time* mtime,
                  const ceph::real_time* mod_ptr,
                  const ceph::real_time* unmod_ptr,
                  bool high_precision_time,
                  const char* if_match,
                  const char* if_nomatch,
                  AttrsMod attrs_mod,
                  bool copy_if_newer,
                  Attrs& attrs,
                  RGWObjCategory category,
                  uint64_t olh_epoch,
                  boost::optional<ceph::real_time> delete_at,
                  std::string* version_id,
                  std::string* tag,
                  std::string* etag,
                  void (*progress_cb)(off_t, void *),
                  void* progress_data,
                  const DoutPrefixProvider* dpp,
                  optional_yield y) override {
    CopyObjectParams p{user, info, &source_zone, dest_object, dest_bucket, src_bucket,
                       &dest_placement, src_mtime, mtime, mod_ptr, unmod_ptr, high_precision_time,
                       if_match, if_nomatch, attrs_mod, copy_if_newer, &attrs, category,
                       olh_epoch, delete_at, version_id, tag, etag, progress_cb, progress_data,
                       dpp, y};
    return copy_object_cb(p);
  }
  MOCK_METHOD(int, copy_object_cb, (const CopyObjectParams&), ());
  MOCK_METHOD(RGWAccessControlPolicy&, get_acl, (), (override));
  MOCK_METHOD(int, set_acl, (const RGWAccessControlPolicy& acl), (override));
  MOCK_METHOD(void, set_atomic, (), (override));
  MOCK_METHOD(bool, is_atomic, (), (override));
  MOCK_METHOD(void, set_prefetch_data, (), (override));
  MOCK_METHOD(bool, is_prefetch_data, (), (override));
  MOCK_METHOD(void, set_compressed, (), (override));
  MOCK_METHOD(bool, is_compressed, (), (override));
  MOCK_METHOD(void, invalidate, (), (override));
  MOCK_METHOD(bool, empty, (), (const, override));
  MOCK_METHOD(const std::string&, get_name, (), (const, override));
  MOCK_METHOD(int, get_obj_state, (const DoutPrefixProvider* dpp, RGWObjState **state, optional_yield y, bool follow_olh), (override));
  MOCK_METHOD(int, set_obj_attrs, (const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y), (override));
  MOCK_METHOD(int, get_obj_attrs, (optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj), (override));
  MOCK_METHOD(int, modify_obj_attrs, (const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp), (override));
  MOCK_METHOD(int, delete_obj_attrs, (const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y), (override));
  MOCK_METHOD(bool, is_expired, (), (override));
  MOCK_METHOD(void, gen_rand_obj_instance_name, (), (override));
  MOCK_METHOD(std::unique_ptr<MPSerializer>, get_serializer, (const DoutPrefixProvider *dpp, const std::string& lock_name), (override));
  MOCK_METHOD(int, transition, (Bucket* bucket, const rgw_placement_rule& placement_rule, const real_time& mtime, uint64_t olh_epoch, const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags), (override));
  MOCK_METHOD(int, transition_to_cloud, (Bucket* bucket, rgw::sal::PlacementTier* tier, rgw_bucket_dir_entry& o, std::set<std::string>& cloud_targets, CephContext* cct, bool update_object, const DoutPrefixProvider* dpp, optional_yield y), (override));
  MOCK_METHOD(bool, placement_rules_match, (rgw_placement_rule& r1, rgw_placement_rule& r2), (override));
  MOCK_METHOD(int, dump_obj_layout, (const DoutPrefixProvider *dpp, optional_yield y, Formatter* f), (override));
  MOCK_METHOD(Attrs&, get_attrs, (), (override));
  MOCK_METHOD(const Attrs&, get_attrs, (), (const, override));
  MOCK_METHOD(int, set_attrs, (Attrs a), (override));
  MOCK_METHOD(bool, has_attrs, (), (override));
  MOCK_METHOD(ceph::real_time, get_mtime, (), (const, override));
  MOCK_METHOD(uint64_t, get_obj_size, (), (const, override));
  MOCK_METHOD(Bucket*, get_bucket, (), (const, override));
  MOCK_METHOD(void, set_bucket, (Bucket* b), (override));
  MOCK_METHOD(std::string, get_hash_source, (), (override));
  MOCK_METHOD(void, set_hash_source, (std::string s), (override));
  MOCK_METHOD(std::string, get_oid, (), (const, override));
  MOCK_METHOD(bool, get_delete_marker, (), (override));
  MOCK_METHOD(bool, get_in_extra_data, (), (override));
  MOCK_METHOD(void, set_in_extra_data, (bool i), (override));
  MOCK_METHOD(void, set_obj_size, (uint64_t s), (override));
  MOCK_METHOD(void, set_name, (const std::string& n), (override));
  MOCK_METHOD(void, set_key, (const rgw_obj_key& k), (override));
  MOCK_METHOD(rgw_obj, get_obj, (), (const, override));
  MOCK_METHOD(int, swift_versioning_restore, (bool& restored, const DoutPrefixProvider* dpp), (override));
  MOCK_METHOD(int, swift_versioning_copy, (const DoutPrefixProvider* dpp, optional_yield y), (override));
  MOCK_METHOD(std::unique_ptr<ReadOp>, get_read_op, (), (override));
  MOCK_METHOD(std::unique_ptr<DeleteOp>, get_delete_op, (), (override));
  MOCK_METHOD(int, omap_get_vals, (const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count, (std::map<std::string, bufferlist>)* m, bool* pmore, optional_yield y), (override));
  MOCK_METHOD(int, omap_get_all, (const DoutPrefixProvider *dpp, (std::map<std::string, bufferlist>)* m, optional_yield y), (override));
  MOCK_METHOD(int, omap_get_vals_by_keys, (const DoutPrefixProvider *dpp, const std::string& oid, const std::set<std::string>& keys, Attrs* vals), (override));
  MOCK_METHOD(int, omap_set_val_by_key, (const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val, bool must_exist, optional_yield y), (override));
  MOCK_METHOD(int, chown, (User& new_user, const DoutPrefixProvider* dpp, optional_yield y), (override));
  MOCK_METHOD(std::unique_ptr<Object>, clone, (), (override));
  MOCK_METHOD(rgw_obj_key&, get_key, (), (override));
  MOCK_METHOD(void, set_instance, (const std::string &i), (override));
  MOCK_METHOD(const std::string&, get_instance, (), (const, override));
  MOCK_METHOD(bool, have_instance, (), (override));
  MOCK_METHOD(void, clear_instance, (), (override));
  MOCK_METHOD(void, print, (std::ostream& out), (const, override));
};

class MockMultipartPart : public MultipartPart {
public:
  MOCK_METHOD(uint32_t, get_num, (), (override));
  MOCK_METHOD(uint64_t, get_size, (), (override));
  MOCK_METHOD(const std::string&, get_etag, (), (override));
  MOCK_METHOD(ceph::real_time&, get_mtime, (), (override));
};

class MockMultipartUpload : public MultipartUpload {
public:
  MOCK_METHOD(const std::string&, get_meta, (), (const, override));
  MOCK_METHOD(const std::string&, get_key, (), (const, override));
  MOCK_METHOD(const std::string&, get_upload_id, (), (const, override));
  MOCK_METHOD(const ACLOwner&, get_owner, (), (const, override));
  MOCK_METHOD(ceph::real_time&, get_mtime, (), (override));
  MOCK_METHOD((std::map<uint32_t, std::unique_ptr<MultipartPart>>)&, get_parts, (), (override));
  MOCK_METHOD(const jspan_context&, get_trace, (), (override));
  MOCK_METHOD(std::unique_ptr<rgw::sal::Object>, get_meta_obj, (), (override));
  MOCK_METHOD(int, init, (const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs), (override));
  MOCK_METHOD(int, list_parts, (const DoutPrefixProvider* dpp, CephContext* cct, int num_parts, int marker, int* next_marker, bool* truncated, bool assume_unsorted), (override));
  MOCK_METHOD(int, abort, (const DoutPrefixProvider* dpp, CephContext* cct), (override));
  // Forwarding override to a mockable helper with a single params object to avoid exceeding gMock arg limits
  int complete(const DoutPrefixProvider* dpp,
               optional_yield y,
               CephContext* cct,
               std::map<int, std::string>& part_etags,
               std::list<rgw_obj_index_key>& remove_objs,
               uint64_t& accounted_size,
               bool& compressed,
               RGWCompressionInfo& cs_info,
               off_t& ofs,
               std::string& tag,
               ACLOwner& owner,
               uint64_t olh_epoch,
               rgw::sal::Object* target_obj) override {
    MultipartCompleteParams p{dpp, y, cct, &part_etags, &remove_objs, &accounted_size,
                              &compressed, &cs_info, &ofs, &tag, &owner, olh_epoch, target_obj};
    return complete_cb(p);
  }
  MOCK_METHOD(int, complete_cb, (const MultipartCompleteParams&), ());
  MOCK_METHOD(int, get_info, (const DoutPrefixProvider *dpp, optional_yield y, rgw_placement_rule** rule, rgw::sal::Attrs* attrs), (override));
  MOCK_METHOD(std::unique_ptr<Writer>, get_writer, (const DoutPrefixProvider *dpp, optional_yield y, rgw::sal::Object* obj, const rgw_user& owner, const rgw_placement_rule *ptail_placement_rule, uint64_t part_num, const std::string& part_num_str), (override));
  MOCK_METHOD(void, print, (std::ostream& out), (const, override));
};

} } // namespace rgw::sal
