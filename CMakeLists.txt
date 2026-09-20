cmake_minimum_required(VERSION 3.5...3.29)
option(DUCKDB_WASM_EXTENSION "Whether compiling for Wasm target" OFF)

###
# Configuration
###
if(NOT DEFINED EXTENSION_NAME)
    message(FATAL_ERROR "DuckDB extension name is required")
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MAJOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MAJOR=${TARGET_DUCKDB_VERSION_MAJOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MINOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MINOR=${TARGET_DUCKDB_VERSION_MINOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_PATCH)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_PATCH=${TARGET_DUCKDB_VERSION_PATCH})
endif()

add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})

if (DEFINED DUCKDB_EXTENSION_API_VERSION_UNSTABLE)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_EXTENSION_API_VERSION_UNSTABLE})
endif()

if(APPLE AND DEFINED ENV{OSX_BUILD_ARCH})
  set(CMAKE_OSX_ARCHITECTURES $ENV{OSX_BUILD_ARCH})
endif()

###
# Build
###
project(${EXTENSION_NAME} LANGUAGES C CXX)

# Create Extension library
set(EXTENSION_SOURCES
        src/lua_extension.cpp
)

# Conflicts with Lua dependency
set(EXTENSION_TARGET_NAME "${EXTENSION_NAME}_ext")
if (DUCKDB_WASM_EXTENSION)
	add_library(${EXTENSION_TARGET_NAME} STATIC ${EXTENSION_SOURCES})
else()
	add_library(${EXTENSION_TARGET_NAME} SHARED ${EXTENSION_SOURCES})
endif()

set_target_properties(${EXTENSION_TARGET_NAME} PROPERTIES OUTPUT_NAME ${EXTENSION_NAME})

# Hide symbols
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(VISIBILITY_INLINES_HIDDEN ON)

# Include own headers
target_include_directories(${EXTENSION_TARGET_NAME} PRIVATE src/include)

# Include DuckDB C API headers
target_include_directories(${EXTENSION_TARGET_NAME} PRIVATE duckdb_capi)

find_package(Lua REQUIRED)
target_include_directories(${EXTENSION_TARGET_NAME} PRIVATE ${LUA_INCLUDE_DIR})
target_link_libraries(${EXTENSION_TARGET_NAME} ${LUA_LIBRARIES})

if(TARGET ${LUA_LIBRARIES})
    set(LUA_LIB_FILE "$<TARGET_FILE:${LUA_LIBRARIES}>")
else()
    set(LUA_LIB_FILE "${LUA_LIBRARIES}")
endif()

if(DUCKDB_WASM_EXTENSION)
    # The Lua library won't be brought into the WASM bundle unless we take additional steps to bundle it in:
    add_custom_command(TARGET ${EXTENSION_TARGET_NAME} POST_BUILD
            COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/scripts/bundle_lua.sh" "${CMAKE_AR}" "${CMAKE_RANLIB}" "$<TARGET_FILE:${EXTENSION_TARGET_NAME}>" "${LUA_LIB_FILE}"
        VERBATIM
        COMMENT "Bundle Lua into the WASM extension archive")
endif()

option(ENABLE_UBSAN "Build with UBSan" OFF)
if (ENABLE_UBSAN)
    message("UBSan enabled")
    target_compile_options(${EXTENSION_TARGET_NAME} PRIVATE -fsanitize=undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
    target_link_options(${EXTENSION_TARGET_NAME} PRIVATE -fsanitize=undefined)
endif()

option(ENABLE_COVERAGE "Build with coverage" OFF)
if (ENABLE_COVERAGE)
    message("Coverage enabled")
    target_compile_options(${EXTENSION_TARGET_NAME} PRIVATE --coverage)
    target_link_options(${EXTENSION_TARGET_NAME} PUBLIC --coverage)
    file(
            GENERATE
            OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/scripts/coverage.sh"
            INPUT "${CMAKE_CURRENT_SOURCE_DIR}/scripts/coverage.sh.in")
    set_property(
            DIRECTORY
            APPEND
            PROPERTY ADDITIONAL_CLEAN_FILES "coverage.info")
    set_property(
            DIRECTORY
            APPEND
            PROPERTY ADDITIONAL_CLEAN_FILES "coverage.cleaned.info")
    set_property(
            DIRECTORY
            APPEND
            PROPERTY ADDITIONAL_CLEAN_FILES "coverage")
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
