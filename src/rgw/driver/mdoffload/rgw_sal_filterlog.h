#pragma once

#include <memory>

#include "rgw_sal_filter.h"

namespace rgw::sal {

class FilterLogUser;
class FilterLogBucket;
class FilterLogObject;
class FilterLogWriter;
class FilterLogMultipartPart;
class FilterLogMultipartUpload;

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
  std::unique_ptr<MultipartUpload> get_multipart_upload(
      const std::string& oid, std::optional<std::string> upload_id = std::nullopt,
      ACLOwner owner = {}, ceph::real_time mtime = real_clock::now()) override;
  int list_multiparts(const DoutPrefixProvider* dpp, const std::string& prefix,
      std::string& marker, const std::string& delim,
      const int& max_uploads,
      std::vector<std::unique_ptr<MultipartUpload>>& uploads,
      std::map<std::string, bool>* common_prefixes,
      bool* is_truncated) override;
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

class FilterLogMultipartPart : public FilterMultipartPart {
public:
  explicit FilterLogMultipartPart(std::unique_ptr<MultipartPart> next_part);
  ~FilterLogMultipartPart() override = default;

  uint32_t get_num() override;
  uint64_t get_size() override;
  const std::string& get_etag() override;
  ceph::real_time& get_mtime() override;
};

class FilterLogMultipartUpload : public FilterMultipartUpload {
public:
  FilterLogMultipartUpload(std::unique_ptr<MultipartUpload> next_upload,
      Bucket* bucket);
  ~FilterLogMultipartUpload() override = default;

  const std::string& get_meta() const override;
  const std::string& get_key() const override;
  const std::string& get_upload_id() const override;
  const ACLOwner& get_owner() const override;
  ceph::real_time& get_mtime() override;
  std::map<uint32_t, std::unique_ptr<MultipartPart>>& get_parts() override;
  const jspan_context& get_trace() override;
  std::unique_ptr<rgw::sal::Object> get_meta_obj() override;
  int init(const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner,
      rgw_placement_rule& dest_placement,
      rgw::sal::Attrs& attrs) override;
  int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
      int num_parts, int marker, int* next_marker, bool* truncated,
      bool assume_unsorted = false) override;
  int abort(const DoutPrefixProvider* dpp, CephContext* cct) override;
  int complete(const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
      std::map<int, std::string>& part_etags,
      std::list<rgw_obj_index_key>& remove_objs,
      uint64_t& accounted_size, bool& compressed,
      RGWCompressionInfo& cs_info, off_t& ofs, std::string& tag,
      ACLOwner& owner, uint64_t olh_epoch,
      rgw::sal::Object* target_obj) override;
  int get_info(const DoutPrefixProvider* dpp, optional_yield y,
      rgw_placement_rule** rule,
      rgw::sal::Attrs* attrs = nullptr) override;
  std::unique_ptr<Writer> get_writer(
      const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Object* obj,
      const rgw_user& owner, const rgw_placement_rule* ptail_placement_rule,
      uint64_t part_num, const std::string& part_num_str) override;
  void print(std::ostream& out) const override;
};

} // namespace rgw::sal
