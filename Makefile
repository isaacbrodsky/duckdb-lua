PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=lua
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

ubsan: export EXT_DEBUG_FLAGS=-DENABLE_UBSAN=1
ubsan: export UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1
ubsan: debug test_debug

coverage:
	EXT_DEBUG_FLAGS=-DENABLE_COVERAGE=1 make debug
	cmake --build cmake_build/debug --config Debug --target clean-coverage
	make test_debug
	cmake --build cmake_build/debug --config Debug --target coverage
