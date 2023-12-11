/**
 * @file rgw_bucketname.h
 * @author André Lucas (alucas@akamai.com)
 * @brief Declarations for BucketName wrapper class.
 * @version 0.1
 * @date 2023-12-06
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include <fmt/format.h>

namespace rgw {

class BucketNameHelperImpl; // Forward declaration.

class BucketNameHelper {

private:
  std::unique_ptr<BucketNameHelperImpl> impl_;

public:
  /*
   * Implementation note: We need to implement the constructor(s) and
   * destructor when we know the size of HandoffHelperImpl. This means we
   * implement in the .cc file, which _does_ include the impl header file.
   * *Don't* include the impl header file in this .h, and don't switch to
   * using the default implementation - it won't compile.
   */
  BucketNameHelper();
  ~BucketNameHelper();

}; // class BucketNameHelper

} // namespace rgw
