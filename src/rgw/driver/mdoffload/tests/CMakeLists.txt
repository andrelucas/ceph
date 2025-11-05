cmake_minimum_required(VERSION 3.14.0)
project(mdoffload-tests)

set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} GMock::GMock)

set(test_rgw_mdoffload_srcs
    test_rgw_mdoffload.cc
    test_rgw_mdoffload_util.h
)

include_directories(${CMAKE_INCLUDE_DIR} ${CMAKE_SOURCE_DIR}/src/test/rgw)

add_executable(unittest_rgw_mdoffload ${test_rgw_mdoffload_srcs})
# No GMock::main, we define our own main().
target_link_libraries(unittest_rgw_mdoffload ${CMAKE_LINK_LIBRARIES} mdoffload_lib)
add_ceph_unittest(unittest_rgw_mdoffload)

set(test_rgw_mdoffload_grpcutil_srcs
    test_rgw_mdoffload_grpcutil.cc
    test_rgw_mdoffload_grpcutil.h
    test_rgw_mdoffload_util.h
)
add_executable(unittest_rgw_mdoffload_grpcutil ${test_rgw_mdoffload_grpcutil_srcs})
target_link_libraries(unittest_rgw_mdoffload_grpcutil ${CMAKE_LINK_LIBRARIES} mdoffload_lib GTest::Main)
add_ceph_unittest(unittest_rgw_mdoffload_grpcutil)
