cmake_minimum_required(VERSION 3.10)
project(arboretum)
message(STATUS "Current project dir: ${PROJECT_SOURCE_DIR}")

###############################################################################
# specify the C++ standard ####################################################
###############################################################################
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} \
-Wall -Wno-comment -Wno-format-zero-length -Wno-unknown-pragmas -Wno-reorder \
-Wno-unused-value -pthread -g -std=c++17 -L/usr/local/lib")
#set(PYTHON_CXX_FLAGS "-I/usr/include/python3.8 -I/usr/include/python3.8
#-fdebug-prefix-map=/build/python3.8-WxaceB/python3.8-3.8.10=.
#-specs=/usr/share/dpkg/no-pie-compile.specs -fstack-protector -Wformat
#-Werror=format-security -fwrapv")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PYTHON_CXX_FLAGS}")
#-L/mydata/vcpkg/installed/x64-linux/lib -lrt

###############################################################################
# Set a default build type if none was specified ##############################
###############################################################################
set(default_build_type "Release")
if(EXISTS ${PROJECT_SOURCE_DIR}/.git)
    set(default_build_type "Debug")
endif()
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
    set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for find_cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo")
endif()
set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE        "-Ofast -DNDEBUG")
# release with debug info
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Ofast -DNDEBUG -g")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

###############################################################################
## get dependencies from git ##################################################
###############################################################################
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
        #https://github.com/google/googletest/archive/refs/tags/release-1.12.1.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# TODO: add flag in cmake to indicate which storage service to use.
#set(Protobuf_INCLUDE_DIR  "/usr/local/include" )
#message(STATUS "protobuf_INCLUDE_DIR: ${Protobuf_INCLUDE_DIR}")
#list(APPEND CMAKE_PREFIX_PATH "${Protobuf_INCLUDE_DIR}")
#find_package(Protobuf REQUIRED)
#include_directories(${Protobuf_INCLUDE_DIRS})
#include_directories(${CMAKE_CURRENT_BINARY_DIR})
#
#find_package(gRPC CONFIG REQUIRED)
#find_program(gRPC_CPP_PLUGIN grpc_cpp_plugin)

find_package(PkgConfig REQUIRED)
pkg_check_modules (JEMALLOC jemalloc)
pkg_search_module(JEMALLOC REQUIRED jemalloc)
include_directories(${JEMALLOC_INCLUDE_DIRS})
message(STATUS "jemalloc path: ${JEMALLOC_INCLUDE_DIRS}")
message(STATUS "jemalloc library: ${JEMALLOC_LIBRARIES}")

# for cpp redis
#add_subdirectory(include/cpp_redis)
#target_include_directories(cpp_redis PUBLIC
#        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/cpp_redis/tacopie
#        /includes>
#        $<INSTALL_INTERFACE:cpp_redis/tacopie>
#        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/cpp_redis
#        /includes>
#        $<INSTALL_INTERFACE:cpp_redis>)

# for azure storage
find_path(WASTORAGE_INCLUDE_DIR was/blob.h)
include_directories(${WASTORAGE_INCLUDE_DIR}) #/mydata/vcpkg/installed/x64-linux/include
message(STATUS "wa storage path: ${WASTORAGE_INCLUDE_DIR}")
find_library(WASTORAGE_LIBRARY azurestorage)
find_library(CPPREST_LIBRARY cpprest)
find_library(BOOST_LOG_LIBRARY boost_log)
find_library(BOOST_THREAD_LIBRARY boost_thread)
find_library(SSL_LIBRARY ssl)
find_library(XML2_LIBRARY xml2)
find_library(LZMA_LIBRARY lzma)
find_library(UUID_LIBRARY uuid)
find_library(Z_LIBRARY z)
find_library(CRYPTO_LIBRARY crypto)

# for python
find_package (Python3 3.8 REQUIRED Development)
include_directories(${Python3_INCLUDE_DIRS})

###############################################################################
# Set configure file ##########################################################
###############################################################################
file(GLOB_RECURSE ALL_CONFIG_FILES ${PROJECT_SOURCE_DIR}/configs/*)
foreach (FILENAME ${ALL_CONFIG_FILES})
    get_filename_component(CONFIGF ${FILENAME} NAME)
    configure_file(${FILENAME} configs/${CONFIGF} COPYONLY)
    message(STATUS "Recognized config file: ${CONFIGF}")
endforeach(FILENAME ${ALL_CONFIG_FILES})

file(GLOB_RECURSE ALL_PY_DEPS_FILES ${PROJECT_SOURCE_DIR}/include/python_deps/*)
foreach (FILENAME ${ALL_PY_DEPS_FILES})
    get_filename_component(PYF ${FILENAME} NAME)
    configure_file(${FILENAME} python_deps/${PYF} COPYONLY)
    message(STATUS "Recognized python modules: ${PYF}")
endforeach(FILENAME ${ALL_PY_DEPS_FILES})

###############################################################################
# Add header files ############################################################
###############################################################################
set(COMMON_HEADER_DIRS
        benchmark
        src
        src/access
        src/common
        src/db
        src/local
        src/remote
        src/txn
        unit_tests)
include_directories(${COMMON_HEADER_DIRS})

###############################################################################
# Add source files ############################################################
###############################################################################
# add common src files
file(GLOB_RECURSE COMMON_CXX_SOURCE_FILES
        ${PROJECT_SOURCE_DIR}/src/common/*.cpp
        ${PROJECT_SOURCE_DIR}/src/db/*.cpp
        ${PROJECT_SOURCE_DIR}/src/local/*.cpp
        ${PROJECT_SOURCE_DIR}/src/remote/*.cpp)

file(GLOB_RECURSE COMMON_CXX_HDR_FILES
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_SOURCE_DIR}/src/common
        ${PROJECT_SOURCE_DIR}/src/db
        ${PROJECT_SOURCE_DIR}/src/local
        ${PROJECT_SOURCE_DIR}/src/remote)

# add individual src files for each benchmark test
file(GLOB_RECURSE YCSB_CXX_SOURCE_FILES
        ${PROJECT_SOURCE_DIR}/benchmark/common/*.cpp
        ${PROJECT_SOURCE_DIR}/benchmark/ycsb/*.cpp)
file(GLOB_RECURSE YCSB_CXX_HDR_FILES
        ${PROJECT_SOURCE_DIR}/benchmark
        ${PROJECT_SOURCE_DIR}/benchmark/common
        ${PROJECT_SOURCE_DIR}/benchmark/ycsb)

# add unit tests
file(GLOB_RECURSE TEST_CXX_SOURCE_FILES
        ${PROJECT_SOURCE_DIR}/unit_tests/*.cpp)

# add experiments
file(GLOB_RECURSE EXP_CXX_SOURCE_FILES
        ${PROJECT_SOURCE_DIR}/experiments/*.cpp)

###############################################################################
# Add clang format ############################################################
###############################################################################
find_program(CLANG_FORMAT_BIN clang-format)
file(GLOB_RECURSE ALL_CXX_FILES
        ${PROJECT_SOURCE_DIR}/*.cpp
        ${PROJECT_SOURCE_DIR}/*.h)
add_custom_target(
        format
        COMMENT "Running: format"
        COMMAND ${CLANG_FORMAT_BIN}
        -i
        -style=file
        ${ALL_CXX_FILES}
)
message(STATUS "[ADDED] format (${CLANG_FORMAT_BIN})")
unset(ALL_CXX_FILES)
unset(CLANG_FORMAT_BIN)

###############################################################################
# Add executable ##############################################################
###############################################################################

message(STATUS "Generating unit tests: ")
include(GoogleTest)
foreach (FILENAME ${TEST_CXX_SOURCE_FILES})
    get_filename_component(EXECUTABLE ${FILENAME} NAME_WE)
    message(STATUS "[unit test] executable: ${EXECUTABLE}")
    add_executable(${EXECUTABLE} ${FILENAME} ${COMMON_CXX_SOURCE_FILES} ${COMMON_CXX_HDR_FILES})
    target_include_directories(${EXECUTABLE} PRIVATE ${COMMON_HEADER_DIRS})
    target_link_libraries(${EXECUTABLE} Python3::Python)
    target_link_libraries(${EXECUTABLE} gtest_main)
    target_link_libraries(${EXECUTABLE} ${JEMALLOC_LIBRARIES})
    #target_link_libraries(${EXECUTABLE} cpp_redis)
    target_link_libraries(${EXECUTABLE}
            ${OPENSSL_LIBRARY}
            ${WASTORAGE_LIBRARY}
            ${CPPREST_LIBRARY}
            ${BOOST_LOG_LIBRARY}
            ${BOOST_THREAD_LIBRARY}
            ${SSL_LIBRARY}
            ${XML2_LIBRARY}
            ${LZMA_LIBRARY}
            ${UUID_LIBRARY}
            ${Z_LIBRARY}
            ${CRYPTO_LIBRARY}
            ${CMAKE_DL_LIBS})
endforeach(FILENAME ${TEST_CXX_SOURCE_FILES})

message(STATUS "Generating experiments: ")
foreach (FILENAME ${EXP_CXX_SOURCE_FILES})
    get_filename_component(EXECUTABLE ${FILENAME} NAME_WE)
    message(STATUS "[experiment] executable: ${EXECUTABLE}")
    add_executable(${EXECUTABLE} ${FILENAME} ${YCSB_CXX_SOURCE_FILES}
            ${COMMON_CXX_SOURCE_FILES} ${COMMON_CXX_HDR_FILES})
    target_include_directories(${EXECUTABLE} PRIVATE
            ${COMMON_HEADER_DIRS} ${YCSB_CXX_HDR_FILES})
    target_link_libraries(${EXECUTABLE} Python3::Python)
    target_link_libraries(${EXECUTABLE} ${JEMALLOC_LIBRARIES})
    #target_link_libraries(${EXECUTABLE} cpp_redis)
    target_link_libraries(${EXECUTABLE}
            ${OPENSSL_LIBRARY}
            ${WASTORAGE_LIBRARY}
            ${CPPREST_LIBRARY}
            ${BOOST_LOG_LIBRARY}
            ${BOOST_THREAD_LIBRARY}
            ${SSL_LIBRARY}
            ${XML2_LIBRARY}
            ${LZMA_LIBRARY}
            ${UUID_LIBRARY}
            ${Z_LIBRARY}
            ${CRYPTO_LIBRARY}
            ${CMAKE_DL_LIBS})
endforeach(FILENAME ${EXP_CXX_SOURCE_FILES})
