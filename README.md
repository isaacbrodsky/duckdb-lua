[![Extension Test](https://github.com/isaacbrodsky/duckdb-lua/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/isaacbrodsky/duckdb-lua/actions/workflows/MainDistributionPipeline.yml)
[![DuckDB Version](https://img.shields.io/static/v1?label=duckdb&message=v1.4.0&color=blue)](https://github.com/duckdb/duckdb/releases/tag/v1.4.0)
[![Lua Version](https://img.shields.io/static/v1?label=lua&message=v5.4.8&color=blue)](https://lua.org/home.html)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

# Lua

This extension adds the embedded scripting language [Lua](https://lua.org) to [DuckDB](https://duckdb.org/). Lua is a powerful, small, embedded, and free scripting language.

Install via community extensions:

```sql
INSTALL lua FROM community; -- First time only
LOAD loa;
```

Evaluate a Lua expression in SQL:
```sql
SELECT lua('return "aa" .. context', "bb");
```

Returns `"aabb"`.

For the context parameter, you can pass in strings, integers, floats, booleans, and so on. Except for JSON input, the return type will be VARCHAR. If an error is encountered, the error message will be returned instead. If you pass in JSON type data, it will be deserialized for you on the Lua side and the Lua return value will also be JSON serialized.

If you wish to rename the `context` variable, use the following option:
```sql
SET lua_context_name = 'ctx';
SELECT lua('return ctx', 'abc');
```

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/lua/lua.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `lua.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a scalar function `lua()` that takes a string arguments and returns a string:

```
D select lua('return "aa" .. "bb"') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ aabb          │
└───────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

# License

duckdb-lua Copyright 2025 Isaac Brodsky. Licensed under the [MIT License](./LICENSE).

[DuckDB](https://github.com/duckdb/duckdb) Copyright 2018-2022 Stichting DuckDB Foundation (MIT License)

[DuckDB extension-template](https://github.com/duckdb/extension-template) Copyright 2018-2022 DuckDB Labs BV (MIT License)

[Lua](https://lua.org/license.html) Copyright © 1994–2025 Lua.org, PUC-Rio. (MIT License)

[dkjson](https://dkolf.de/dkjson-lua/) Copyright (C) 2010-2024 David Heiko Kolf (MIT License)
