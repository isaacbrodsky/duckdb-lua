#!/usr/bin/env bash

set -euo pipefail

AR="$1"
RANLIB="$2"
EXTENSION_LIB="$3"
LUA_LIB="$4"

sentinel="$("$AR" t "$LUA_LIB" | head -n 1)"
if "$AR" t "$EXTENSION_LIB" | grep -qxF "$sentinel"; then
  echo "$EXTENSION_LIB contains $LUA_LIB already"
  exit 0
fi

temp_dir="$(mktemp -d)"
trap 'rm -rf "$temp_dir"' EXIT

pushd "$temp_dir"

"$AR" x "$LUA_LIB"
"$AR" q "$EXTENSION_LIB" *.o
"$RANLIB" "$EXTENSION_LIB"

popd
