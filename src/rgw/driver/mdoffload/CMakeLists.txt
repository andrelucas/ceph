cmake_minimum_required(VERSION 3.14.0)
project(mdoffload)

set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} gtest)

set(mdoffload_srcs
    rgw_sal_mdoffload.cc)

include_directories(${CMAKE_INCLUDE_DIR})

find_package(gtest QUIET)
if(WITH_TESTS)
    add_subdirectory(tests)
else()
	message(WARNING "Gtest not enabled")
endif()

include_directories(${CMAKE_INCLUDE_DIR})
add_library(mdoffload_lib STATIC ${mdoffload_srcs})
target_link_libraries(mdoffload_lib
    ${CMAKE_LINK_LIBRARIES}
    ${rgw_libs}
    rgw_common
)
target_include_directories(mdoffload_lib
    PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
    PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/driver/mdoffload"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
