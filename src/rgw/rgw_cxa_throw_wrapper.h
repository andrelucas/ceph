/**
 * @file rgw_cxa_throw_wrapper.h
 * @author André Lucas (alucas@akamai.com)
 * @brief Declarations for __cxa_throw wrapper function
 * @version 0.1
 * @date 2024-11-01
 *
 * @copyright Copyright (c) 2024
 *
 */

#pragma once

#import <atomic>
#import <typeinfo>

extern std::atomic<uint64_t> _cxa_throw_exception_count;
extern std::atomic<uint64_t> _cxa_throw_nested_exception_count;

// Original __cxa_throw function signature
extern "C" {
void __real___cxa_throw(void* ex, std::type_info* info, void (*dest)(void*));
}
