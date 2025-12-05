#pragma once

#include <memory>

#include "rgw_sal_filter.h"

namespace rgw::sal {

class FilterLogUser;
class FilterLogBucket;
class FilterLogObject;
class FilterLogWriter;

class FilterLogDriver : public FilterDriver {
public:
  explicit FilterLogDriver(Driver* next_driver);
  ~FilterLogDriver() override = default;

  int initialize(CephContext* cct, const DoutPrefixProvider* dpp) override;
  const std::string get_name() const override;
  std::unique_ptr<User> get_user(const rgw_user& u) override;
  int get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key,
                             optional_yield y, std::unique_ptr<User>* user) override;
  int get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email,
                        optional_yield y, std::unique_ptr<User>* user) override;
  int get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str,
                        optional_yield y, std::unique_ptr<User>* user) override;
  std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  int get_bucket(User* u, const RGWBucketInfo& info,
                 std::unique_ptr<Bucket>* bucket) override;
  int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b,
                 std::unique_ptr<Bucket>* bucket, optional_yield y) override;
  int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant,
                 const std::string& name, std::unique_ptr<Bucket>* bucket,
                 optional_yield y) override;
  std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider* dpp,
                                            optional_yield y, rgw::sal::Object* obj,
                                            const rgw_user& owner,
                                            const rgw_placement_rule* ptail_placement_rule,
                                            const std::string& unique_tag,
                                            uint64_t position,
                                            uint64_t* cur_accounted_size) override;
  std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider* dpp,
                                            optional_yield y, rgw::sal::Object* obj,
                                            const rgw_user& owner,
                                            const rgw_placement_rule* ptail_placement_rule,
                                            uint64_t olh_epoch,
                                            const std::string& unique_tag) override;
};

class FilterLogUser : public FilterUser {
public:
  explicit FilterLogUser(std::unique_ptr<User> next_user);
  ~FilterLogUser() override = default;

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
};

class FilterLogBucket : public FilterBucket {
public:
  FilterLogBucket(std::unique_ptr<Bucket> next_bucket, User* user);
  ~FilterLogBucket() override = default;

  std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
  std::unique_ptr<Bucket> clone() override;
};

class FilterLogObject : public FilterObject {
public:
  class FilterLogReadOp : public FilterObject::FilterReadOp {
  public:
    explicit FilterLogReadOp(std::unique_ptr<Object::ReadOp> next_op);
    ~FilterLogReadOp() override = default;

    int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
    int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y,
        const DoutPrefixProvider* dpp) override;
    int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
        RGWGetDataCB* cb, optional_yield y) override;
    int get_attr(const DoutPrefixProvider* dpp, const char* name,
        bufferlist& dest, optional_yield y) override;
  };

  class FilterLogDeleteOp : public FilterObject::FilterDeleteOp {
  public:
    explicit FilterLogDeleteOp(std::unique_ptr<Object::DeleteOp> next_op);
    ~FilterLogDeleteOp() override = default;

    int delete_obj(const DoutPrefixProvider* dpp, optional_yield y,
        uint32_t flags) override;
  };

  explicit FilterLogObject(std::unique_ptr<Object> next_object);
  FilterLogObject(std::unique_ptr<Object> next_object, Bucket* bucket);
  ~FilterLogObject() override = default;

  std::unique_ptr<Object> clone() override;
  std::unique_ptr<Object::ReadOp> get_read_op() override;
  std::unique_ptr<Object::DeleteOp> get_delete_op() override;
  int delete_object(const DoutPrefixProvider* dpp, optional_yield y,
                    uint32_t flags) override;
  int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate,
                     Completions* aio, bool keep_index_consistent,
                     optional_yield y) override;
  int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                    Attrs* delattrs, optional_yield y) override;
  int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                    rgw_obj* target_obj = nullptr) override;
  int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                       optional_yield y, const DoutPrefixProvider* dpp) override;
  int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                       optional_yield y) override;
};

class FilterLogWriter : public FilterWriter {
public:
  FilterLogWriter(std::unique_ptr<Writer> next_writer, Object* obj);
  ~FilterLogWriter() override = default;

  int prepare(optional_yield y) override;
  int process(bufferlist&& data, uint64_t offset) override;
  int complete(size_t accounted_size, const std::string& etag,
               ceph::real_time* mtime, ceph::real_time set_mtime,
               std::map<std::string, bufferlist>& attrs,
               ceph::real_time delete_at, const char* if_match,
               const char* if_nomatch, const std::string* user_data,
               rgw_zone_set* zones_trace, bool* canceled,
               optional_yield y, uint32_t flags) override;
};

} // namespace rgw::sal
