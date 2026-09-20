#!/usr/bin/env bash

set -euo pipefail

AR="$1"
RANLIB="$2"
EXTENSION_LIB="$3"
LUA_LIB="$4"

if "$AR" t "$EXTENSION_LIB" | grep -qxF "lua_pcall"; then
  echo "$EXTENSION_LIB contains $LUA_LIB already"
  exit 0
fi

temp_dir="$(mktemp -d)"
trap 'rm -rf "temp_dir"' EXIT

pushd "$temp_dir"

"$AR" x "$LUA_LIB"
"$AR" q "$EXTENSION_LIB" *.o
"$RANLIB" "$EXTENSION_LIB"

popd
