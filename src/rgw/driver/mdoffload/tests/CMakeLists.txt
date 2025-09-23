cmake_minimum_required(VERSION 3.14.0)
project(mdoffload-tests)

set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} gtest)

set(mdoffload_tests_srcs
    test_mdoffload.cc)

include_directories(${CMAKE_INCLUDE_DIR})

add_executable(unittest_rgw_mdoffload ${mdoffload_tests_srcs})
target_link_libraries(unittest_rgw_mdoffload ${CMAKE_LINK_LIBRARIES} mdoffload_lib gtest_main)
add_ceph_unittest(unittest_rgw_mdoffload)
