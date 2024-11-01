/**
 * @file rgw_cxa_throw_wrapper.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Definition for __cxa_throw wrapper function
 * @version 0.1
 * @date 2024-11-01
 *
 * @copyright Copyright (c) 2024
 *
 */

#include "rgw_cxa_throw_wrapper.h"

#include <iostream>

std::atomic<uint64_t> _cxa_throw_exception_count = 0;
std::atomic<uint64_t> _cxa_throw_nested_exception_count = 0;

// Custom __cxa_throw function
extern "C" {
void cxa_throw_gdb_hook()
{
  // this is just to be able to set breakpoint with gdb
  std::cerr << "Inside gdb hook.\n";
  return;
}

void __wrap___cxa_throw(void* ex, std::type_info* info, void (*dest)(void*))
{
  _cxa_throw_exception_count++;
  // Check if an exception is already in flight
  if (std::uncaught_exceptions() > 0) {
    _cxa_throw_nested_exception_count++;
    std::cerr << "Exception already in flight!\n";
    cxa_throw_gdb_hook();
  }

  // Otherwise, proceed with the normal exception throw
  // XXX do we really want to print for every exception?
  //   std::cerr << "proceed with the normal exception throw.\n";
  __real___cxa_throw(ex, info, dest);
}
}
