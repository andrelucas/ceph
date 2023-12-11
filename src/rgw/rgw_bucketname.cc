/**
 * @file rgw_bucketname.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Definitions for BucketNameHelper wrapper class.
 * @version 0.1
 * @date 2023-12-06
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "rgw_bucketname.h"

#include <memory>

#include "rgw_bucketname_impl.h"

namespace rgw {

// This has to be here, in a .cc file where we know the size of
// BucketNameHelperImpl. It can't be in the header file. See
// https://www.fluentcpp.com/2017/09/22/make-pimpl-using-unique_ptr/ .
BucketNameHelper::BucketNameHelper()
    : impl_ {
      std::make_unique<BucketNameHelperImpl>()
    }
{
}

// This has to be here, in a .cc file where we know the size of
// BucketNameHelperImpl. It can't be in the header file. See
// https://www.fluentcpp.com/2017/09/22/make-pimpl-using-unique_ptr/ .
BucketNameHelper::~BucketNameHelper() { }

} // namespace rgw
