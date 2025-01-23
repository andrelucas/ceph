// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <absl/strings/numbers.h>
#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/token_functions.hpp>
#include <boost/tokenizer.hpp>
#include <cstdint>
#include <fmt/format.h>
#include <stdexcept>
#include <string>

#include "cls/rgw/cls_rgw_types.h"
#include "common/async/yield_context.h"
#include "common/dout.h"
#include "rgw_b64.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_rest_storequery.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace rgw {

/***************************************************************************/

// RGWStoreQueryOp_Base

void RGWStoreQueryOp_Base::send_response_pre()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  auto ret = RGWHandler_REST::reallocate_formatter(s, RGWFormat::JSON);
  if (ret != 0) {
    ldpp_dout(this, 20) << "failed to set formatter to JSON" << dendl;
    set_req_state_err(s, -EINVAL);
  }
  dump_errno(s);
  end_header(s, this, "application/json");
  dump_start(s);
}

void RGWStoreQueryOp_Base::send_response_post()
{
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWStoreQueryOp_Base::send_response()
{
  send_response_pre();
  send_response_json();
  send_response_post();
}

/***************************************************************************/

// RGWStoreQueryOp_Ping

void RGWStoreQueryOp_Ping::execute(optional_yield y)
{
  ldpp_dout(this, 20) << fmt::format(FMT_STRING("{}: {}({})"), typeid(this).name(),
      __func__, request_id_)
                      << dendl;
  // This can't fail.
  op_ret = 0;
}

void RGWStoreQueryOp_Ping::send_response_json()
{
  s->formatter->open_object_section("StoreQueryPingResult");
  s->formatter->dump_string("request_id", request_id_);
  s->formatter->close_section();
}

/***************************************************************************/

// RGWStoreQueryOp_ObjectStatus

/**
 * @brief Query already-existing objects, or delete markers.
 *
 * Perform a 'regular' query, returning either pre-existing objects or (in
 * versioning-enabled buckets) delete markers for previously-existing objects.
 * In either case, the object is deemed to be found.
 *
 * We check for the current version and stop further searching the moment we
 * find it.
 *
 * However, since rgw::sal::Bucket::list() queries on a prefix not a key, we
 * also check for an exact key match each time.
 *
 * Note that op_ret will be set <0 in case failures other than 'not found'.
 * This indicates that we should abort the query process.
 *
 * @param y optional yield object.
 * @return true Success. The object was found, in this case it is either
 * present or a delete marker for it exists. op_ret==0.
 * @return false Failure. If op_ret==0, the object was simply not found. If
 * op_ret<0, a failure occurred.
 */
bool RGWStoreQueryOp_ObjectStatus::execute_simple_query(optional_yield y)
{
  bool found = false;

  rgw::sal::Bucket::ListParams params {};
  params.prefix = object_key_name_;
  // We want results even if the last object is a delete marker. In a bucket
  // without versioning a query for a deleted or nonexistent object will
  // return zero objects, for which we'll return ENOENT.
  params.list_versions = true;
  // We always want an ordered list of objects. This is the default atow.
  params.allow_unordered = false;

  do {
    rgw::sal::Bucket::ListResults results;
    // This is the 'page size' for the bucket list. We're unlikely to have
    // more than a thousand versions, but we're querying a prefix and there
    // could easily be a *lot* of objects with the given prefix.
    constexpr int version_query_max = 100;

    ldpp_dout(this, 20) << fmt::format(
        FMT_STRING("issue bucket list() query next_marker={}"),
        params.marker.name)
                        << dendl;
    // NOTE: rgw::sal::RadosBucket::list() updates params.marker as it
    // goes. This isn't how list_multiparts() works.
    auto ret = s->bucket->list(this, params, version_query_max, results, y);

    if (ret < 0) {
      op_ret = ret;
      ldpp_dout(this, 2) << "sal bucket->list query failed ret=" << ret
                         << dendl;
      break;
    }

    if (results.objs.size() == 0) {
      // EOF. Exit the simple search loop.
      ldpp_dout(this, 20) << fmt::format(FMT_STRING("bucket list() prefix='{}' EOF"),
          object_key_name_)
                          << dendl;
      break;

    } else {
      for (size_t n = 0; n < results.objs.size(); n++) {
        auto& obj = results.objs[n];
        // Check for exact key match - we searched a prefix.
        if (obj.key.name != object_key_name_) {
          ldpp_dout(this, 20)
              << fmt::format(FMT_STRING("ignore non-exact match key={}"), obj.key.name)
              << dendl;
          continue;
        }

        ldpp_dout(this, 20)
            << fmt::format(FMT_STRING("obj {}/{}: exists={} current={} delete_marker={}"),
                   n, results.objs.size(), obj.exists, obj.is_current(),
                   obj.is_delete_marker())
            << dendl;
        if (obj.is_current()) {
          found = true;
          object_deleted_ = obj.is_delete_marker();
          if (!object_deleted_) {
            object_size_ = obj.meta.size;
          }
          break;
        }
      }
    }
  } while (!found);

  if (found) {
    ldpp_dout(this, 20) << fmt::format(FMT_STRING("found key={} in standard path"),
        object_key_name_)
                        << dendl;
    op_ret = 0;
    return true;
  }
  return false;
}

/**
 * @brief Query in-progress multipart uploads for our key.
 *
 * Query in-process multipart uploads for an exact match for our key. This can
 * be an expensive index query if there are a lot of in-flight mp uploads.
 *
 * rgw::sal::Bucket::list_multiparts() queries on a prefix (not a full key),
 * so we check for an exact key match each time.
 *
 * Note that op_ret will be set <0 in case failures other than 'not found'.
 * This indicates that we should abort the query process.
 *
 * @param y optional yield object.
 * @return true Success, the object was found. op_ret==0.
 * @return false Failure. If op_ret==0, the object was simply not found. If
 * op_ret<0, an error occurred.
 */
bool RGWStoreQueryOp_ObjectStatus::execute_mpupload_query(optional_yield y)
{
  bool found = false;

  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads {};
  std::string marker { "" };
  std::string delimiter { "" };
  constexpr int mp_query_max = 100;
  bool is_truncated; // Must be present, pointer to this is unconditionally
                     // written by list_multiparts().

  do {
    // Re-initialise this every run. We can only see if the query is complete
    // across multiple list_multiparts() by checking if this is empty.
    // However, nothing in list_multiparts() clears it.
    uploads.clear();

    ldpp_dout(this, 20) << fmt::format(
        FMT_STRING("issue list_multiparts() query marker='{}'"),
        marker)
                        << dendl;
    // Note that 'marker' is an inout param that we'll need for subsequent
    // queries.
    auto ret = s->bucket->list_multiparts(this, object_key_name_, marker,
        delimiter, mp_query_max, uploads,
        nullptr, &is_truncated);
    if (ret < 0) {
      ldpp_dout(this, 2) << "list_multiparts() failed with code " << ret
                         << dendl;
      op_ret = ret;
      break;
    }

    if (uploads.size() == 0) {
      ldpp_dout(this, 20) << fmt::format(FMT_STRING("list_multiparts() prefix='{}' EOF"),
          object_key_name_)
                          << dendl;
      break;
    }

    for (auto const& upload : uploads) {
      if (upload->get_key() == object_key_name_) {
        object_mpuploading_ = true;
        object_mpupload_id_ = upload->get_upload_id();
        ldpp_dout(this, 20)
            << fmt::format(
                   FMT_STRING("multipart upload found for object={} upload_id='{}'"),
                   upload->get_key(), object_mpupload_id_)
            << dendl;
        found = true;
        break;
      }
    }
  } while (!found);

  if (found) {
    ldpp_dout(this, 20) << fmt::format(FMT_STRING("found key={} in mp upload path"),
        object_key_name_)
                        << dendl;
    op_ret = 0;
    return true;
  }
  return false;
}

void RGWStoreQueryOp_ObjectStatus::execute(optional_yield y)
{
  ceph_assert(s->bucket != nullptr);
  bucket_name_ = rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name);
  object_key_name_ = s->object->get_key().name;

  ldpp_dout(this, 20) << fmt::format(FMT_STRING("{}: {} (bucket='{}' object='{}')"),
      typeid(this).name(), __func__,
      bucket_name_, object_key_name_)
                      << dendl;

  // op_ret is used to signal a real failure, meaning we should not continue.
  op_ret = 0;

  if (execute_simple_query(y) || op_ret < 0) {
    return;
  }
  if (execute_mpupload_query(y) || op_ret < 0) {
    return;
  }

  // Not found anywhere.
  ldpp_dout(this, 2) << "key not found" << dendl;
  op_ret = -ENOENT;
  return;
}

void RGWStoreQueryOp_ObjectStatus::send_response_json()
{
  s->formatter->open_object_section("StoreQueryObjectStatusResult");
  s->formatter->open_object_section("Object");
  s->formatter->dump_string("bucket", bucket_name_);
  s->formatter->dump_string("key", object_key_name_);
  s->formatter->dump_bool("deleted", object_deleted_);
  s->formatter->dump_bool("multipart_upload_in_progress", object_mpuploading_);
  if (object_mpuploading_) {
    s->formatter->dump_string("multipart_upload_id", object_mpupload_id_);
  }
  if (!object_deleted_ && !object_mpuploading_) {
    s->formatter->dump_string("version_id", version_id_);
    s->formatter->dump_int("size", static_cast<int64_t>(object_size_));
  }
  s->formatter->close_section(); // Object
  s->formatter->close_section(); // StoreQueryObjectStatusResult
}

/***************************************************************************/

// RGWStoreQueryOp_ObjectList

bool RGWStoreQueryOp_ObjectList::execute_query(optional_yield y)
{
  // The ListParams persists across multiple requests.
  rgw::sal::Bucket::ListParams params {};

  // Fill in the contination token if we need to.
  if (marker_.has_value()) {
    try {
      std::string init_marker = from_base64(*marker_);
      params.marker = rgw_obj_key(init_marker);
      ldpp_dout(this, 10) << fmt::format(FMT_STRING("continuation token '{}' decoded as {}"), *marker_, init_marker)
                          << dendl;
    } catch (std::exception& e) {
      // We can't catch boost::archive::archive_exception specifically, it
      // doesn't link and I'm not fixing the CMake just for one exception.
      ldpp_dout(this, 0) << fmt::format(FMT_STRING("failed to decode continuation token: '{}'"), e.what())
                         << dendl;
      op_ret = -EINVAL;
      return false;
    }
  }

  // No prefix for a complete list of the bucket.
  params.prefix = "";
  // We want results even if the last object is a delete marker. In a bucket
  // without versioning a query for a deleted or nonexistent object will
  // return zero objects, for which we'll return ENOENT.
  params.list_versions = true;
  // It appears pagination works fine with unordered queries.
  params.allow_unordered = true;

  // Cap the number of entries we'll return to our LIST_QUERY_SIZE_HARD_LIMIT.
  // We can experiment with this in a lab, but in production let's make sure
  // we don't overtax the system.
  uint64_t query_max = std::min(max_entries_, LIST_QUERY_SIZE_HARD_LIMIT);
  if (query_max < max_entries_) {
    ldpp_dout(this, 5) << fmt::format(FMT_STRING("max_entries {} is above the hard limit, restricting query_max to {}"), max_entries_, query_max)
                       << dendl;
  }

  bool seen_eof = false;
  std::string next_marker;

  // Reserve space for the maximum number of entries we might return. This is
  // a compromise - we could reallocate as we issue queries against the
  // backend, but this will lead to heap churn and copies. This feels like the
  // proper balance to me; reserve enough space for the maximum number of
  // items we're going to return (which we don't in any case allow to be
  // /insanely/ high), and reserve it exactly once.
  items_.reserve(max_entries_);

  stats_.entries_max = max_entries_;

  // Loop until we've filled the user's requested number of entries, or we hit
  // EOF.
  while (items_.size() < max_entries_) {
    rgw::sal::Bucket::ListResults results;

    ldpp_dout(this, 20) << fmt::format(
        FMT_STRING("issue bucket list() query query_max={} next_marker={}"),
        query_max, params.marker.name)
                        << dendl;
    // Call our indirection for rgw::sal::Bucket::list(). Note that
    // rgw::sal::RadosBucket::list() updates params.marker as it goes. This
    // isn't how list_multiparts() works, don't get caught.
    auto ret = _list_impl(params, results, query_max, y);

    if (ret < 0) {
      op_ret = ret;
      ldpp_dout(this, 2) << "SAL bucket->list() query failed ret=" << ret
                         << dendl;
      break;
    }

    ldpp_dout(this, 20) << fmt::format(FMT_STRING("SAL bucket->list() returned {} items"), results.objs.size())
                        << dendl;

    if (results.objs.size() == 0) {
      // EOF. Exit the loop.
      ldpp_dout(this, 20) << fmt::format(FMT_STRING("SAL bucket->list() EOF items_.size()={}"), items_.size()) << dendl;
      seen_eof = true;
      break;
    }

    // Loop over the results of s->bucket->list().
    for (size_t n = 0; n < results.objs.size(); n++) {
      auto& obj = results.objs[n];

      ldpp_dout(this, 20)
          << fmt::format(FMT_STRING("obj {}/{}: key={} exists={} current={} delete_marker={}"),
                 n + 1, results.objs.size(), obj.key.name, obj.exists, obj.is_current(),
                 obj.is_delete_marker())
          << dendl;

      // We're only interested in the current (most recent) version of the
      // object.
      stats_.sal_seen++;

      if (obj.is_current()) {
        stats_.sal_current++;
        item_type item { obj.key.name };
        item.set_deleted(obj.is_delete_marker());
        if (obj.is_delete_marker()) {
          stats_.sal_deleted++;
        } else {
          // Only non-deleted items should have a size.
          item.set_size(obj.meta.size);
        }
        items_.push_back(item);
        stats_.entries_actual++;
      }
      if (obj.exists) {
        stats_.sal_exists++;
      }

      // Extra action if we've reached the caller's size limit.
      if (items_.size() == max_entries_) {
        // If we filled items_ set the token for next time. It's ok if it's
        // actually the end of the list - the next query will just have zero
        // items.
        next_marker = results.objs[n].key.name;
        ldpp_dout(this, 20) << fmt::format(FMT_STRING("max_entries reached, next={}"), next_marker) << dendl;
        break;
      }
    }
  }

  // s->bucket->list() can fail. We rely on op_ret being properly set at the
  // point of failure.
  if (op_ret < 0) {
    return false;
  }

  if (!seen_eof && !next_marker.empty()) {
    // If there are more results, we need to safely encode the continuation
    // marker and return it to the user. This is done by setting
    // return_marker_, which will be dumped in send_response_json().
    //
    // Note that it's safe to use to_base64() here. Even though it looks like it
    // will insert line breaks, it's actually a template and the default line
    // wrap width is std::numeric_limits<int>::max().
    std::string encoded_marker;
    try {
      encoded_marker = to_base64(next_marker);
    } catch (std::runtime_error& e) {
      ldpp_dout(this, 0) << fmt::format(FMT_STRING("failed to encode continuation token: '{}'"), e.what())
                         << dendl;
      op_ret = -EINVAL;
      return false;
    }
    ldpp_dout(this, 20) << fmt::format(FMT_STRING("EOF not reached, next_marker {}"), params.marker.name)
                        << dendl;
    ldpp_dout(this, 5) << fmt::format(FMT_STRING("EOF not reached, continuation token {}"), encoded_marker)
                       << dendl;
    set_return_marker(encoded_marker);
  }

  return true;
}

void RGWStoreQueryOp_ObjectList::execute(optional_yield y)
{
  if (!execute_query(y)) {
    // rely on execute_query() setting op_ret appropriately.
    ldpp_dout(this, 1) << "execute_query() failed" << dendl;
  }
}

void RGWStoreQueryOp_ObjectList::send_response_json()
{
  auto f = s->formatter;
  f->open_object_section("StoreQueryObjectListResult");

  f->open_array_section("Objects");
  for (const auto& item : items_) {
    item.dump(f);
  }
  f->close_section(); // Objects

  f->open_object_section("Stats");
  stats_.dump(f);
  f->close_section(); // Stats

  if (return_marker_.has_value()) {
    f->dump_string("NextToken", *return_marker_);
  }
  f->close_section(); // StoreQueryObjectListResult
}

void RGWStoreQueryOp_ObjectList::Item::dump(Formatter* f) const
{
  f->open_object_section("Object");
  f->dump_string("key", key_);
  // Only dump optional attributes if they've been given values.
  if (is_deleted_.has_value() && *is_deleted_) {
    // We only dump the attribute if it's set and true.
    f->dump_bool("deleted", *is_deleted_);
  }
  if (size_.has_value()) {
    // Size of zero is a value value.
    f->dump_unsigned("size", *size_);
  }
  f->close_section();
}

/***************************************************************************/

// RGWStoreQueryOp_MPUploadList

bool RGWStoreQueryOp_MPUploadList::execute_query(optional_yield y)
{
  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads {};

  std::string marker;

  if (marker_.has_value()) {
    try {
      marker = from_base64(*marker_);
      ldpp_dout(this, 10) << fmt::format(FMT_STRING("continuation token '{}' decoded as {}"), *marker_, marker)
                          << dendl;
    } catch (std::exception& e) {
      // We can't catch boost::archive::archive_exception specifically, it
      // doesn't link and I'm not fixing the CMake just for one exception.
      ldpp_dout(this, 0) << fmt::format(FMT_STRING("failed to decode continuation token: '{}'"), e.what())
                         << dendl;
      op_ret = -EINVAL;
      return false;
    }
  }

  bool is_truncated; // Must be present, pointer to this is unconditionally
                     // written by list_multiparts().

  uint64_t query_max = std::min(max_entries_, LIST_MULTIPARTS_QUERY_SIZE_HARD_LIMIT);
  if (query_max < max_entries_) {
    ldpp_dout(this, 5) << fmt::format(FMT_STRING("max_entries {} is above the hard limit, restricting query_max to {}"), max_entries_, query_max)
                       << dendl;
  }

  bool seen_eof = false;
  std::string next_marker;

  // Reserve space for the maximum number of entries we might return. This is
  // a compromise - we could reallocate as we issue queries against the
  // backend, but this will lead to heap churn and copies. This feels like the
  // proper balance to me; reserve enough space for the maximum number of
  // items we're going to return (which we don't in any case allow to be
  // /insanely/ high), and reserve it exactly once.
  items_.reserve(max_entries_);

  while (items_.size() < max_entries_) {
    // Re-initialise this every run. We can only see if the query is complete
    // across multiple list_multiparts() by checking if this is empty.
    // However, nothing in list_multiparts() clears it.
    uploads.clear();

    ldpp_dout(this, 20) << fmt::format(
        FMT_STRING("issue list_multiparts() query marker='{}'"), marker)
                        << dendl;
    ldpp_dout(this, 20) << fmt::format(
        FMT_STRING("issue list_multiparts() query query_max={} marker={}"), query_max, marker)
                        << dendl;

    // Call our indirection for rgw::sal::Bucket::list_multiparts(). Few
    // notes:
    // - marker is an inout parameter that we need for pagination.
    // - is_truncated must not be null (the standard indirection will assert
    //   if it is, but the reason is that the underlying implementation
    //   doesn't do a nullptr check before asserting).
    // - Don't make any assumptions about how many records will be returned,
    //   except that it will be <= query_max.
    auto ret = _list_multiparts_impl("", marker, "", uploads, &is_truncated, query_max);

    if (ret < 0) {
      ldpp_dout(this, 2) << "list_multiparts() failed with code " << ret
                         << dendl;
      op_ret = ret;
      break;
    }

    if (uploads.size() == 0) {
      ldpp_dout(this, 20) << fmt::format(FMT_STRING("SAL list_multiparts() EOF items_.size()={}"), items_.size())
                          << dendl;
      seen_eof = true;
      break;
    }

    for (auto const& upload : uploads) {
      auto& key = upload->get_key();
      ldpp_dout(this, 20)
          << fmt::format(FMT_STRING("obj: key={} upload_id={}"), key, upload->get_upload_id())
          << dendl;

      item_type item { key };
      items_.push_back(item);

      // Extra action if we've reached the caller's size limit.
      if (items_.size() == max_entries_) {
        // If we filled items_ set the token for next time. It's ok if it's
        // actually the end of the list - the next query will just have zero
        // items.
        next_marker = marker;
        ldpp_dout(this, 20) << fmt::format(FMT_STRING("max_entries reached, next={}"), next_marker) << dendl;
        break;
      }
    }
  }

  if (op_ret < 0) {
    return false;
  }

  if (!seen_eof && !next_marker.empty()) {
    // If there are more results, we need to safely encode the continuation
    // marker and return it to the user. This is done by setting
    // return_marker_, which will be dumped in send_response_json().
    //
    // Note that it's safe to use to_base64() here. Even though it looks like it
    // will insert line breaks, it's actually a template and the default line
    // wrap width is std::numeric_limits<int>::max().
    std::string encoded_marker;
    try {
      encoded_marker = to_base64(next_marker);
    } catch (std::exception& e) {
      ldpp_dout(this, 0) << fmt::format(FMT_STRING("failed to encode continuation token: '{}'"), e.what())
                         << dendl;
      op_ret = -EINVAL;
      return false;
    }
    ldpp_dout(this, 20) << fmt::format(FMT_STRING("EOF not reached, next_marker {}"), marker)
                        << dendl;
    ldpp_dout(this, 5) << fmt::format(FMT_STRING("EOF not reached, continuation token {}"), encoded_marker)
                       << dendl;
    set_return_marker(encoded_marker);
  }

  return true;
}

void RGWStoreQueryOp_MPUploadList::execute(optional_yield y)
{
  if (!execute_query(y)) {
    // rely on execute_query() setting op_ret appropriately.
    ldpp_dout(this, 1) << "execute_query() failed" << dendl;
  }
}

void RGWStoreQueryOp_MPUploadList::send_response_json()
{
  auto f = s->formatter;
  f->open_object_section("StoreQueryMPUploadListResult");
  f->open_array_section("Objects");
  for (const auto& item : items_) {
    item.dump(f);
  }
  f->close_section(); // Objects
  if (return_marker_.has_value()) {
    f->dump_string("NextToken", *return_marker_);
  }
  f->close_section(); // StoreQueryMPUploadListResult
}

void RGWStoreQueryOp_MPUploadList::Item::dump(Formatter* f) const
{
  f->open_object_section("Object");
  f->dump_string("key", key_);
  if (num_parts_.has_value()) {
    f->dump_unsigned("num_parts", *num_parts_);
  }
  f->close_section(); // Object
}

/***************************************************************************/

namespace ba = boost::algorithm;

static const char* SQ_HEADER = "HTTP_X_RGW_STOREQUERY";
static const char* HEADER_LC = "x-rgw-storequery";

void RGWSQHeaderParser::reset()
{
  op_ = nullptr;
  command_ = "";
  param_.clear();
}

bool RGWSQHeaderParser::tokenize(const DoutPrefixProvider* dpp,
    const std::string& input)
{
  if (input.empty()) {
    ldpp_dout(dpp, 0) << fmt::format(FMT_STRING("illegal empty {} header"), HEADER_LC)
                      << dendl;
    return false;
  }
  if (input.size() > RGWSQMaxHeaderLength) {
    ldpp_dout(dpp, 0) << fmt::format(
        FMT_STRING("{} header exceeds maximum length of {} chars"),
        HEADER_LC, RGWSQMaxHeaderLength)
                      << dendl;
    return false;
  }
  // Enforce ASCII-7 non-control characters.
  if (!std::all_of(input.cbegin(), input.cend(),
          [](auto c) { return c >= ' '; })) {
    ldpp_dout(dpp, 0) << fmt::format(FMT_STRING("Illegal character found in {}"), HEADER_LC)
                      << dendl;
    return false;
  }

  // Only debug the header contents after canonicalising it.
  ldpp_dout(dpp, 20) << fmt::format(FMT_STRING("header {}: '{}'"), HEADER_LC, input)
                     << dendl;

  try {
    // Use boost::tokenizer to split into space-separated fields, allowing
    // double-quoted fields to contain spaces.
    boost::escaped_list_separator<char> els("\\", " ", "\"");
    boost::tokenizer<boost::escaped_list_separator<char>> tok { input, els };
    bool first = true;
    for (const auto& t : tok) {
      if (first) {
        // Always lowercase the command name.
        command_ = ba::to_lower_copy(t);
        first = false;
        continue;
      }
      param_.push_back(std::string { t });
    }
    return true;
  } catch (const boost::escaped_list_error& e) {
    ldpp_dout(dpp, 0) << fmt::format(FMT_STRING("Failed to parse storequery header: {}"), e.what()) << dendl;
    return false;
  }
}

bool RGWSQHeaderParser::parse(const DoutPrefixProvider* dpp,
    const std::string& input,
    RGWSQHandlerType handler_type)
{
  op_ = nullptr;
  if (!tokenize(dpp, input)) {
    return false;
  }
  if (command_.empty()) {
    ldpp_dout(dpp, 0) << fmt::format(FMT_STRING("{}: no command found"), HEADER_LC)
                      << dendl;
    return false;
  }
  if (command_ == "objectstatus") {
    // ObjectStatus command.
    //
    if (handler_type != RGWSQHandlerType::Obj) {
      ldpp_dout(dpp, 0)
          << fmt::format(FMT_STRING("{}: ObjectStatus only applies in an Object context"),
                 HEADER_LC)
          << dendl;
      return false;
    }
    if (param_.size() != 0) {
      ldpp_dout(dpp, 0)
          << fmt::format(
                 "{}: malformed ObjectStatus command (expected zero args)",
                 HEADER_LC)
          << dendl;
      return false;
    }
    // The naked new is part of the interface.
    op_ = new RGWStoreQueryOp_ObjectStatus();
    return true;

  } else if (command_ == "objectlist") {
    // ObjectList command.
    //
    if (handler_type != RGWSQHandlerType::Bucket) {
      ldpp_dout(dpp, 0)
          << fmt::format(FMT_STRING("{}: ObjectList only applies in an Bucket context"),
                 HEADER_LC)
          << dendl;
      return false;
    }
    if (param_.size() < 1 || param_.size() > 2) {
      ldpp_dout(dpp, 0)
          << fmt::format(
                 "{}: malformed ObjectList command (expected one or two args)",
                 HEADER_LC)
          << dendl;
      return false;
    }
    uint64_t max_entries;
    if (!absl::SimpleAtoi(param_[0], &max_entries)) {
      ldpp_dout(dpp, 0)
          << fmt::format(FMT_STRING("{}: malformed ObjectList command (expected integer in first parameter)"),
                 HEADER_LC)
          << dendl;
      return false;
    }
    std::optional<std::string> marker;
    if (param_.size() == 2) {
      marker = param_[1];
    }
    // The naked new is part of the interface.
    op_ = new RGWStoreQueryOp_ObjectList(max_entries, marker);
    return true;

  } else if (command_ == "mpuploadlist") {
    // mpuploadlist command.
    //
    if (handler_type != RGWSQHandlerType::Bucket) {
      ldpp_dout(dpp, 0)
          << fmt::format(FMT_STRING("{}: mpuploadlist only applies in an Bucket context"),
                 HEADER_LC)
          << dendl;
      return false;
    }
    if (param_.size() < 1 || param_.size() > 2) {
      ldpp_dout(dpp, 0)
          << fmt::format(
                 "{}: malformed mpuploadlist command (expected one or two args)",
                 HEADER_LC)
          << dendl;
      return false;
    }
    uint64_t max_entries;
    if (!absl::SimpleAtoi(param_[0], &max_entries)) {
      ldpp_dout(dpp, 0)
          << fmt::format(FMT_STRING("{}: malformed mpuploadlist command (expected integer in first parameter)"),
                 HEADER_LC)
          << dendl;
      return false;
    }
    std::optional<std::string> marker;
    if (param_.size() == 2) {
      marker = param_[1];
    }
    // The naked new is part of the interface.
    op_ = new RGWStoreQueryOp_MPUploadList(max_entries, marker);
    return true;

  } else if (command_ == "ping") {
    // Ping command.
    //
    // Allow ping from any handler type - it doesn't matter!
    if (param_.size() != 1) {
      ldpp_dout(dpp, 0) << fmt::format(
          "{}: malformed Ping command (expected one arg)",
          HEADER_LC)
                        << dendl;
      return false;
    }
    // The naked new is part of the interface.
    op_ = new RGWStoreQueryOp_Ping(param_[0]);
    return true;
  }
  return false;
}

bool RGWHandler_REST_StoreQuery_S3::is_storequery_request(const req_state* s)
{
  if (s->op != OP_GET) {
    return false;
  }
  auto hdr = s->info.env->get(SQ_HEADER, nullptr);
  return (hdr != nullptr);
}

RGWOp* RGWHandler_REST_StoreQuery_S3::op_get()
{
  // Handler selection (RGWHandler_REST_S3::get_handler()) checks for the
  // storequery header. If we get here without that header, something is
  // wrong.
  ceph_assert(is_storequery_request(s));

  auto hdr = s->info.env->get(SQ_HEADER, nullptr);
  DoutPrefix dpp { g_ceph_context, ceph_subsys_rgw, "storequery_parse " };

  // If we fail to parse now, we return nullptr to indicate 'method not
  // allowed'. The logs will contain an explanation, but there's no way (using
  // op_get(), an override) to influence the output returned to the user
  // without much more substantial code changes.
  auto p = RGWSQHeaderParser();
  if (!p.parse(&dpp, hdr, handler_type_)) {
    ldpp_dout(&dpp, 0) << fmt::format(FMT_STRING("{}: parser failure"), HEADER_LC) << dendl;
    return nullptr;
  }
  p.op()->init(driver, s, this);
  return p.op();
}

RGWOp* RGWHandler_REST_StoreQuery_S3::op_put()
{
  // We don't handle PUT requests yet.
  return nullptr;
}
RGWOp* RGWHandler_REST_StoreQuery_S3::op_delete()
{
  // We don't handle DELETE requests yet.
  return nullptr;
}

int RGWHandler_REST_StoreQuery_S3::init_permissions(RGWOp* op, optional_yield y)
{
  ldpp_dout(op, 20) << "init_permissions()" << dendl;
  const auto dpp = op;

  int ret = 0;

  if (handler_type_ == RGWSQHandlerType::Service) {
    // Service handlers don't care about buckets or objects.
    return ret;
  }

  // This is essentially copied (somewhat truncated) from
  // RGWHandler::rgw_build_bucket_policies(). When changing Ceph versions, it
  // would be smart to check if the code still matches.
  if (!s->bucket_name.empty()) {
    s->bucket_exists = true;

    /* This is the only place that s->bucket is created.  It should never be
     * overwritten. */
    ret = driver->get_bucket(dpp, s->user.get(), rgw_bucket(s->bucket_tenant, s->bucket_name, s->bucket_instance_id), &s->bucket, y);
    if (ret < 0) {
      if (ret != -ENOENT) {
        string bucket_log;
        bucket_log = rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name);
        ldpp_dout(dpp, 0) << "NOTICE: couldn't get bucket from bucket_name (name="
                          << bucket_log << ")" << dendl;
        return ret;
      }
      s->bucket_exists = false;
      return -ERR_NO_SUCH_BUCKET;
    }

    s->bucket_mtime = s->bucket->get_modification_time();
    s->bucket_attrs = s->bucket->get_attrs();

    // There's no need to load an object if we're a bucket-type handler.
    if (handler_type_ != RGWSQHandlerType::Obj) {
      return ret;
    }
    if (!rgw::sal::Object::empty(s->object.get())) {
      s->object->set_bucket(s->bucket.get());
    }
  }
  return ret;
}

} // namespace rgw
