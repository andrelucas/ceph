#define dout_subsys ceph_subsys_rgw

#include "rgw_sal_filterlog.h"

#include <fmt/format.h>
#include <string_view>

#include "common/dout.h"

namespace rgw::sal {
namespace {

inline User* unwrap_user(User* user) {
  if (!user) {
    return nullptr;
  }
  if (auto* filter_user = dynamic_cast<FilterUser*>(user)) {
    return filter_user->get_next();
  }
  return user;
}

inline Object* unwrap_object(Object* object) {
  if (!object) {
    return nullptr;
  }
  if (auto* filter_object = dynamic_cast<FilterObject*>(object)) {
    return filter_object->get_next();
  }
  return object;
}

inline void log_call(const DoutPrefixProvider* dpp, const std::string_view name) {
  ldpp_dout(dpp, 20) << fmt::format(FMT_STRING("{}"), name) << dendl;
}

inline void log_call(const std::string_view name) {
  ldout(g_ceph_context, 20) << fmt::format(FMT_STRING("{}"), name) << dendl;
}

} // namespace

FilterLogDriver::FilterLogDriver(Driver* next_driver)
    : FilterDriver(next_driver) {}

int FilterLogDriver::initialize(CephContext* cct, const DoutPrefixProvider* dpp) {
  log_call(dpp, "FilterLogDriver::initialize");
  return FilterDriver::initialize(cct, dpp);
}

const std::string FilterLogDriver::get_name() const {
  log_call("FilterLogDriver::get_name");
  return fmt::format(FMT_STRING("filterlog<{}>"), next->get_name());
}

std::unique_ptr<User> FilterLogDriver::get_user(const rgw_user& u) {
  log_call("FilterLogDriver::get_user");
  auto inner = next->get_user(u);
  if (!inner) {
    return nullptr;
  }
  return std::make_unique<FilterLogUser>(std::move(inner));
}

int FilterLogDriver::get_user_by_access_key(const DoutPrefixProvider* dpp,
                                            const std::string& key,
                                            optional_yield y,
                                            std::unique_ptr<User>* user) {
  log_call(dpp, "FilterLogDriver::get_user_by_access_key");
  std::unique_ptr<User> inner;
  int ret = next->get_user_by_access_key(dpp, key, y, &inner);
  if (ret < 0) {
    return ret;
  }
  user->reset(new FilterLogUser(std::move(inner)));
  return 0;
}

int FilterLogDriver::get_user_by_email(const DoutPrefixProvider* dpp,
                                       const std::string& email,
                                       optional_yield y,
                                       std::unique_ptr<User>* user) {
  log_call(dpp, "FilterLogDriver::get_user_by_email");
  std::unique_ptr<User> inner;
  int ret = next->get_user_by_email(dpp, email, y, &inner);
  if (ret < 0) {
    return ret;
  }
  user->reset(new FilterLogUser(std::move(inner)));
  return 0;
}

int FilterLogDriver::get_user_by_swift(const DoutPrefixProvider* dpp,
                                       const std::string& user_str,
                                       optional_yield y,
                                       std::unique_ptr<User>* user) {
  log_call(dpp, "FilterLogDriver::get_user_by_swift");
  std::unique_ptr<User> inner;
  int ret = next->get_user_by_swift(dpp, user_str, y, &inner);
  if (ret < 0) {
    return ret;
  }
  user->reset(new FilterLogUser(std::move(inner)));
  return 0;
}

std::unique_ptr<Object> FilterLogDriver::get_object(const rgw_obj_key& k) {
  log_call("FilterLogDriver::get_object");
  auto object = next->get_object(k);
  if (!object) {
    return nullptr;
  }
  return std::make_unique<FilterLogObject>(std::move(object));
}

int FilterLogDriver::get_bucket(User* u, const RGWBucketInfo& info,
                                std::unique_ptr<Bucket>* bucket) {
  log_call("FilterLogDriver::get_bucket(info)");
  std::unique_ptr<Bucket> inner;
  int ret = next->get_bucket(unwrap_user(u), info, &inner);
  if (ret < 0) {
    return ret;
  }
  bucket->reset(new FilterLogBucket(std::move(inner), u));
  return 0;
}

int FilterLogDriver::get_bucket(const DoutPrefixProvider* dpp, User* u,
                                const rgw_bucket& b,
                                std::unique_ptr<Bucket>* bucket,
                                optional_yield y) {
  log_call(dpp, "FilterLogDriver::get_bucket(bucket)");
  std::unique_ptr<Bucket> inner;
  int ret = next->get_bucket(dpp, unwrap_user(u), b, &inner, y);
  if (ret < 0) {
    return ret;
  }
  bucket->reset(new FilterLogBucket(std::move(inner), u));
  return 0;
}

int FilterLogDriver::get_bucket(const DoutPrefixProvider* dpp, User* u,
                                const std::string& tenant, const std::string& name,
                                std::unique_ptr<Bucket>* bucket,
                                optional_yield y) {
  log_call(dpp, "FilterLogDriver::get_bucket(tenant)");
  std::unique_ptr<Bucket> inner;
  int ret = next->get_bucket(dpp, unwrap_user(u), tenant, name, &inner, y);
  if (ret < 0) {
    return ret;
  }
  bucket->reset(new FilterLogBucket(std::move(inner), u));
  return 0;
}

std::unique_ptr<Writer> FilterLogDriver::get_append_writer(
    const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Object* obj,
    const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule,
    const std::string& unique_tag, uint64_t position,
    uint64_t* cur_accounted_size) {
  log_call(dpp, "FilterLogDriver::get_append_writer");
  auto writer = next->get_append_writer(dpp, y, unwrap_object(obj), owner,
                                        ptail_placement_rule, unique_tag, position,
                                        cur_accounted_size);
  if (!writer) {
    return nullptr;
  }
  return std::make_unique<FilterLogWriter>(std::move(writer), obj);
}

std::unique_ptr<Writer> FilterLogDriver::get_atomic_writer(
    const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Object* obj,
    const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t olh_epoch,
    const std::string& unique_tag) {
  log_call(dpp, "FilterLogDriver::get_atomic_writer");
  auto writer = next->get_atomic_writer(dpp, y, unwrap_object(obj), owner,
                                        ptail_placement_rule, olh_epoch,
                                        unique_tag);
  if (!writer) {
    return nullptr;
  }
  return std::make_unique<FilterLogWriter>(std::move(writer), obj);
}

FilterLogUser::FilterLogUser(std::unique_ptr<User> next_user)
    : FilterUser(std::move(next_user)) {}

std::unique_ptr<User> FilterLogUser::clone() {
  log_call("FilterLogUser::clone");
  auto* base = get_next();
  if (!base) {
    return nullptr;
  }
  auto inner = base->clone();
  return std::make_unique<FilterLogUser>(std::move(inner));
}

int FilterLogUser::list_buckets(const DoutPrefixProvider* dpp,
                                const std::string& marker,
                                const std::string& end_marker, uint64_t max,
                                bool need_stats, BucketList& buckets,
                                optional_yield y) {
  log_call(dpp, "FilterLogUser::list_buckets");
  BucketList inner;
  int ret = next->list_buckets(dpp, marker, end_marker, max, need_stats, inner, y);
  if (ret < 0) {
    return ret;
  }
  buckets.clear();
  buckets.set_truncated(inner.is_truncated());
  for (auto& entry : inner.get_buckets()) {
    buckets.add(std::make_unique<FilterLogBucket>(std::move(entry.second), this));
  }
  return 0;
}

int FilterLogUser::create_bucket(
    const DoutPrefixProvider* dpp, const rgw_bucket& b,
    const std::string& zonegroup_id, rgw_placement_rule& placement_rule,
    std::string& swift_ver_location, const RGWQuotaInfo* pquota_info,
    const RGWAccessControlPolicy& policy, Attrs& attrs, RGWBucketInfo& info,
    obj_version& ep_objv, bool exclusive, bool obj_lock_enabled, bool* existed,
    req_info& req_info, std::unique_ptr<Bucket>* bucket, optional_yield y) {
  log_call(dpp, "FilterLogUser::create_bucket");
  std::unique_ptr<Bucket> inner;
  int ret = next->create_bucket(dpp, b, zonegroup_id, placement_rule,
                                swift_ver_location, pquota_info, policy, attrs, info,
                                ep_objv, exclusive, obj_lock_enabled, existed,
                                req_info, &inner, y);
  if (ret < 0) {
    return ret;
  }
  if (bucket) {
    bucket->reset(new FilterLogBucket(std::move(inner), this));
  }
  return 0;
}

FilterLogBucket::FilterLogBucket(std::unique_ptr<Bucket> next_bucket, User* user)
    : FilterBucket(std::move(next_bucket), user) {}

std::unique_ptr<Object> FilterLogBucket::get_object(const rgw_obj_key& key) {
  log_call("FilterLogBucket::get_object");
  auto object = next->get_object(key);
  if (!object) {
    return nullptr;
  }
  return std::make_unique<FilterLogObject>(std::move(object), this);
}

std::unique_ptr<Bucket> FilterLogBucket::clone() {
  log_call("FilterLogBucket::clone");
  if (!next) {
    return nullptr;
  }
  auto cloned = next->clone();
  if (!cloned) {
    return nullptr;
  }
  return std::make_unique<FilterLogBucket>(std::move(cloned), get_owner());
}

std::unique_ptr<MultipartUpload> FilterLogBucket::get_multipart_upload(
    const std::string& oid, std::optional<std::string> upload_id,
    ACLOwner owner, ceph::real_time mtime)
{
  log_call("FilterLogBucket::get_multipart_upload");
  auto upload = next->get_multipart_upload(oid, std::move(upload_id), owner, mtime);
  if (!upload) {
    return nullptr;
  }
  return std::make_unique<FilterLogMultipartUpload>(std::move(upload), this);
}

int FilterLogBucket::list_multiparts(
    const DoutPrefixProvider* dpp, const std::string& prefix,
    std::string& marker, const std::string& delim, const int& max_uploads,
    std::vector<std::unique_ptr<MultipartUpload>>& uploads,
    std::map<std::string, bool>* common_prefixes, bool* is_truncated)
{
  log_call(dpp, "FilterLogBucket::list_multiparts");
  std::vector<std::unique_ptr<MultipartUpload>> inner;
  int ret = next->list_multiparts(dpp, prefix, marker, delim, max_uploads,
      inner, common_prefixes, is_truncated);
  if (ret < 0) {
    return ret;
  }
  for (auto& upload : inner) {
    uploads.emplace_back(
        std::make_unique<FilterLogMultipartUpload>(std::move(upload), this));
  }
  return 0;
}

FilterLogObject::FilterLogObject(std::unique_ptr<Object> next_object)
    : FilterObject(std::move(next_object)) {}

FilterLogObject::FilterLogObject(std::unique_ptr<Object> next_object,
                                 Bucket* bucket)
    : FilterObject(std::move(next_object), bucket) {}

std::unique_ptr<Object> FilterLogObject::clone() {
  log_call("FilterLogObject::clone");
  auto* base = get_next();
  if (!base) {
    return nullptr;
  }
  auto cloned = base->clone();
  if (!cloned) {
    return nullptr;
  }
  return std::make_unique<FilterLogObject>(std::move(cloned), get_bucket());
}

std::unique_ptr<Object::ReadOp> FilterLogObject::get_read_op() {
  log_call("FilterLogObject::get_read_op");
  auto* base = get_next();
  if (!base) {
    return nullptr;
  }
  auto op = base->get_read_op();
  if (!op) {
    return nullptr;
  }
  return std::make_unique<FilterLogReadOp>(std::move(op));
}

std::unique_ptr<Object::DeleteOp> FilterLogObject::get_delete_op() {
  log_call("FilterLogObject::get_delete_op");
  auto* base = get_next();
  if (!base) {
    return nullptr;
  }
  auto op = base->get_delete_op();
  if (!op) {
    return nullptr;
  }
  return std::make_unique<FilterLogDeleteOp>(std::move(op));
}

int FilterLogObject::delete_object(const DoutPrefixProvider* dpp,
                                   optional_yield y, uint32_t flags) {
  log_call(dpp, "FilterLogObject::delete_object");
  return FilterObject::delete_object(dpp, y, flags);
}

int FilterLogObject::delete_obj_aio(const DoutPrefixProvider* dpp,
                                    RGWObjState* astate, Completions* aio,
                                    bool keep_index_consistent,
                                    optional_yield y) {
  log_call(dpp, "FilterLogObject::delete_obj_aio");
  return FilterObject::delete_obj_aio(dpp, astate, aio, keep_index_consistent, y);
}

int FilterLogObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                                   Attrs* delattrs, optional_yield y) {
  log_call(dpp, "FilterLogObject::set_obj_attrs");
  return FilterObject::set_obj_attrs(dpp, setattrs, delattrs, y);
}

int FilterLogObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                   rgw_obj* target_obj) {
  log_call(dpp, "FilterLogObject::get_obj_attrs");
  return FilterObject::get_obj_attrs(y, dpp, target_obj);
}

int FilterLogObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                                      optional_yield y,
                                      const DoutPrefixProvider* dpp) {
  log_call(dpp, "FilterLogObject::modify_obj_attrs");
  return FilterObject::modify_obj_attrs(attr_name, attr_val, y, dpp);
}

int FilterLogObject::delete_obj_attrs(const DoutPrefixProvider* dpp,
                                      const char* attr_name, optional_yield y) {
  log_call(dpp, "FilterLogObject::delete_obj_attrs");
  return FilterObject::delete_obj_attrs(dpp, attr_name, y);
}

FilterLogObject::FilterLogReadOp::FilterLogReadOp(
    std::unique_ptr<Object::ReadOp> next_op)
    : FilterObject::FilterReadOp(std::move(next_op))
{
}

int FilterLogObject::FilterLogReadOp::prepare(optional_yield y,
    const DoutPrefixProvider* dpp)
{
  log_call(dpp, "FilterLogReadOp::prepare");
  return FilterObject::FilterReadOp::prepare(y, dpp);
}

int FilterLogObject::FilterLogReadOp::read(int64_t ofs, int64_t end,
    bufferlist& bl, optional_yield y,
    const DoutPrefixProvider* dpp)
{
  log_call(dpp, "FilterLogReadOp::read");
  return FilterObject::FilterReadOp::read(ofs, end, bl, y, dpp);
}

int FilterLogObject::FilterLogReadOp::iterate(const DoutPrefixProvider* dpp,
    int64_t ofs, int64_t end,
    RGWGetDataCB* cb,
    optional_yield y)
{
  log_call(dpp, "FilterLogReadOp::iterate");
  return FilterObject::FilterReadOp::iterate(dpp, ofs, end, cb, y);
}

int FilterLogObject::FilterLogReadOp::get_attr(const DoutPrefixProvider* dpp,
    const char* name,
    bufferlist& dest,
    optional_yield y)
{
  log_call(dpp, "FilterLogReadOp::get_attr");
  return FilterObject::FilterReadOp::get_attr(dpp, name, dest, y);
}

FilterLogObject::FilterLogDeleteOp::FilterLogDeleteOp(
    std::unique_ptr<Object::DeleteOp> next_op)
    : FilterObject::FilterDeleteOp(std::move(next_op))
{
}

int FilterLogObject::FilterLogDeleteOp::delete_obj(
    const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags)
{
  log_call(dpp, "FilterLogDeleteOp::delete_obj");
  return FilterObject::FilterDeleteOp::delete_obj(dpp, y, flags);
}

FilterLogWriter::FilterLogWriter(std::unique_ptr<Writer> next_writer, Object* obj)
    : FilterWriter(std::move(next_writer), obj) {}

int FilterLogWriter::prepare(optional_yield y) {
  log_call("FilterLogWriter::prepare");
  return FilterWriter::prepare(y);
}

int FilterLogWriter::process(bufferlist&& data, uint64_t offset) {
  log_call("FilterLogWriter::process");
  return FilterWriter::process(std::move(data), offset);
}

int FilterLogWriter::complete(size_t accounted_size, const std::string& etag,
                              ceph::real_time* mtime, ceph::real_time set_mtime,
                              std::map<std::string, bufferlist>& attrs,
                              ceph::real_time delete_at, const char* if_match,
                              const char* if_nomatch, const std::string* user_data,
                              rgw_zone_set* zones_trace, bool* canceled,
                              optional_yield y, uint32_t flags) {
  log_call("FilterLogWriter::complete");
  return FilterWriter::complete(accounted_size, etag, mtime, set_mtime, attrs,
                                delete_at, if_match, if_nomatch, user_data,
                                zones_trace, canceled, y, flags);
}

FilterLogMultipartPart::FilterLogMultipartPart(
    std::unique_ptr<MultipartPart> next_part)
    : FilterMultipartPart(std::move(next_part))
{
}

uint32_t FilterLogMultipartPart::get_num()
{
  log_call("FilterLogMultipartPart::get_num");
  return FilterMultipartPart::get_num();
}

uint64_t FilterLogMultipartPart::get_size()
{
  log_call("FilterLogMultipartPart::get_size");
  return FilterMultipartPart::get_size();
}

const std::string& FilterLogMultipartPart::get_etag()
{
  log_call("FilterLogMultipartPart::get_etag");
  return FilterMultipartPart::get_etag();
}

ceph::real_time& FilterLogMultipartPart::get_mtime()
{
  log_call("FilterLogMultipartPart::get_mtime");
  return FilterMultipartPart::get_mtime();
}

FilterLogMultipartUpload::FilterLogMultipartUpload(
    std::unique_ptr<MultipartUpload> next_upload, Bucket* bucket)
    : FilterMultipartUpload(std::move(next_upload), bucket)
{
}

const std::string& FilterLogMultipartUpload::get_meta() const
{
  log_call("FilterLogMultipartUpload::get_meta");
  return FilterMultipartUpload::get_meta();
}

const std::string& FilterLogMultipartUpload::get_key() const
{
  log_call("FilterLogMultipartUpload::get_key");
  return FilterMultipartUpload::get_key();
}

const std::string& FilterLogMultipartUpload::get_upload_id() const
{
  log_call("FilterLogMultipartUpload::get_upload_id");
  return FilterMultipartUpload::get_upload_id();
}

const ACLOwner& FilterLogMultipartUpload::get_owner() const
{
  log_call("FilterLogMultipartUpload::get_owner");
  return FilterMultipartUpload::get_owner();
}

ceph::real_time& FilterLogMultipartUpload::get_mtime()
{
  log_call("FilterLogMultipartUpload::get_mtime");
  return FilterMultipartUpload::get_mtime();
}

std::map<uint32_t, std::unique_ptr<MultipartPart>>&
FilterLogMultipartUpload::get_parts()
{
  log_call("FilterLogMultipartUpload::get_parts");
  return FilterMultipartUpload::get_parts();
}

const jspan_context& FilterLogMultipartUpload::get_trace()
{
  log_call("FilterLogMultipartUpload::get_trace");
  return FilterMultipartUpload::get_trace();
}

std::unique_ptr<rgw::sal::Object> FilterLogMultipartUpload::get_meta_obj()
{
  log_call("FilterLogMultipartUpload::get_meta_obj");
  auto meta = next->get_meta_obj();
  if (!meta) {
    return nullptr;
  }
  return std::make_unique<FilterLogObject>(std::move(meta), bucket);
}

int FilterLogMultipartUpload::init(const DoutPrefixProvider* dpp,
    optional_yield y, ACLOwner& owner,
    rgw_placement_rule& dest_placement,
    rgw::sal::Attrs& attrs)
{
  log_call(dpp, "FilterLogMultipartUpload::init");
  return FilterMultipartUpload::init(dpp, y, owner, dest_placement, attrs);
}

int FilterLogMultipartUpload::list_parts(
    const DoutPrefixProvider* dpp, CephContext* cct, int num_parts, int marker,
    int* next_marker, bool* truncated, bool assume_unsorted)
{
  log_call(dpp, "FilterLogMultipartUpload::list_parts");
  int ret = next->list_parts(dpp, cct, num_parts, marker, next_marker,
      truncated, assume_unsorted);
  if (ret < 0) {
    return ret;
  }
  parts.clear();
  for (auto& entry : next->get_parts()) {
    parts.emplace(entry.first,
        std::make_unique<FilterLogMultipartPart>(
            std::move(entry.second)));
  }
  return 0;
}

int FilterLogMultipartUpload::abort(const DoutPrefixProvider* dpp,
    CephContext* cct)
{
  log_call(dpp, "FilterLogMultipartUpload::abort");
  return FilterMultipartUpload::abort(dpp, cct);
}

int FilterLogMultipartUpload::complete(
    const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
    std::map<int, std::string>& part_etags,
    std::list<rgw_obj_index_key>& remove_objs, uint64_t& accounted_size,
    bool& compressed, RGWCompressionInfo& cs_info, off_t& ofs,
    std::string& tag, ACLOwner& owner, uint64_t olh_epoch,
    rgw::sal::Object* target_obj)
{
  log_call(dpp, "FilterLogMultipartUpload::complete");
  return FilterMultipartUpload::complete(dpp, y, cct, part_etags, remove_objs,
      accounted_size, compressed, cs_info,
      ofs, tag, owner, olh_epoch,
      target_obj);
}

int FilterLogMultipartUpload::get_info(const DoutPrefixProvider* dpp,
    optional_yield y,
    rgw_placement_rule** rule,
    rgw::sal::Attrs* attrs)
{
  log_call(dpp, "FilterLogMultipartUpload::get_info");
  return FilterMultipartUpload::get_info(dpp, y, rule, attrs);
}

std::unique_ptr<Writer> FilterLogMultipartUpload::get_writer(
    const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Object* obj,
    const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t part_num,
    const std::string& part_num_str)
{
  log_call(dpp, "FilterLogMultipartUpload::get_writer");
  auto writer = next->get_writer(dpp, y, unwrap_object(obj), owner,
      ptail_placement_rule, part_num, part_num_str);
  if (!writer) {
    return nullptr;
  }
  return std::make_unique<FilterLogWriter>(std::move(writer), obj);
}

void FilterLogMultipartUpload::print(std::ostream& out) const
{
  log_call("FilterLogMultipartUpload::print");
  FilterMultipartUpload::print(out);
}

} // namespace rgw::sal

#undef dout_subsys
