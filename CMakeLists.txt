cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME lua)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(Lua REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/lua_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link Lua in both the static library as the loadable extension
target_include_directories(${EXTENSION_NAME} PRIVATE ${LUA_INCLUDE_DIR})
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${LUA_INCLUDE_DIR})
target_link_libraries(${EXTENSION_NAME} ${LUA_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${LUA_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

option(ENABLE_COVERAGE "Build with coverage" OFF)
if (ENABLE_COVERAGE)
    message("Coverage enabled")
    target_compile_options(${EXTENSION_NAME} PRIVATE --coverage)
    target_compile_options(${LOADABLE_EXTENSION_NAME} PRIVATE --coverage)
    target_link_options(${EXTENSION_NAME} PUBLIC --coverage)
    target_link_options(${LOADABLE_EXTENSION_NAME} PUBLIC --coverage)
    file(
            GENERATE
            OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/scripts/coverage.sh"
            INPUT "${CMAKE_CURRENT_SOURCE_DIR}/scripts/coverage.sh.in")
    set_property(
            DIRECTORY
            APPEND
            PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "coverage.info")
    set_property(
            DIRECTORY
            APPEND
            PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "coverage.cleaned.info")
    set_property(
            DIRECTORY
            APPEND
            PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "coverage")
    add_custom_target(
            coverage
            COMMAND
            bash "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/scripts/coverage.sh"
            "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")
    add_custom_target(
            clean-coverage
            # Before running coverage, clear all counters
            COMMAND lcov --directory
            '${CMAKE_CURRENT_BINARY_DIR}' --zerocounters
            COMMENT "Zeroing counters")
endif()
