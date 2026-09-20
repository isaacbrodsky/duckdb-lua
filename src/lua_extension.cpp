#define DUCKDB_EXTENSION_MAIN

#include "lua_extension.hpp"
#include "dkjson.hpp"
#include <string>
#include <type_traits>
#include <cassert>

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}

DUCKDB_EXTENSION_EXTERN

const auto BUFFER_NAME = "line";

inline static std::string ReadLuaResponse(duckdb_function_info info, lua_State *L, bool error) {
	std::string resultStr;
	if (lua_isnoneornil(L, -1)) {
		resultStr = "nil";
	} else if (lua_isstring(L, -1)) {
		// Also covers number and integer types, since lua_isstring checks whether
		// the type can be coerced to string, and that includes numbers by default.
		resultStr = lua_tostring(L, -1);
	} else if (lua_isboolean(L, -1)) {
		resultStr = lua_toboolean(L, -1) ? "true" : "false";
	} else {
		auto type = lua_type(L, -1);
		resultStr = std::string("Unknown type: ") + lua_typename(L, type);
	}
	lua_pop(L, 1);

	if (error) {
		auto errorStr = "Lua Error: " + resultStr;
		duckdb_scalar_function_set_error(info, errorStr.c_str());
	}

	return resultStr;
}

/** Cache the last loaded script to avoid re-parsing if not needed */
class LastScript {
public:
	LastScript(duckdb_function_info info, lua_State *L)
	    : lastScriptData(nullptr), lastScriptLen(0), hasLastScript(false), info(info), L(L) {
	}

	/** Replace the cached script on the Lua stack if needed.
	 * Call with push=false if the script doesn't need to be used at this point,
	 * and then call PushScript later.
	 * If false is returned, an error was set on the DuckDB function info.
	 */
	bool Advance(duckdb_string_t *script, bool push = true) {
		if (!hasLastScript || ScriptsDiffer(script)) {
			if (hasLastScript) {
				lua_pop(L, 1);
			}

			auto loadError =
			    luaL_loadbuffer(L, duckdb_string_t_data(script), duckdb_string_t_length(*script), BUFFER_NAME);
			if (loadError) {
				ReadLuaResponse(info, L, loadError);
				return false;
			}

			lastScriptData = duckdb_string_t_data(script);
			lastScriptLen = duckdb_string_t_length(*script);
			lastScriptTop = lua_gettop(L);
			hasLastScript = true;
		}
		if (push) {
			// Duplicate script
			PushScript();
		}
		return true;
	}

	inline void PushScript() {
		lua_pushvalue(L, lastScriptTop);
	}

private:
	bool ScriptsDiffer(duckdb_string_t *script) {
		if (duckdb_string_t_length(*script) != lastScriptLen) {
			return true;
		}

		// Check for the same script by checking if duckdb reused the same string object
		// (i.e. for constant string columns)
		if (duckdb_string_t_data(script) == lastScriptData) {
			return false;
		}

		// Note: lastScriptLen must be the same as duckdb_string_t_length, per above
		if (memcmp(duckdb_string_t_data(script), lastScriptData, lastScriptLen) == 0) {
			return false;
		}

		return true;
	}

	duckdb_function_info info;
	lua_State *L;
	
	/** Whether a script has been saved - lastScript* fields are only valid if this is true */
	bool hasLastScript;
	const char *lastScriptData;
	uint32_t lastScriptLen;
	/** Lua stack position of the last script */
	int lastScriptTop;
};

void LuaScalarFun(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t inputSize = duckdb_data_chunk_get_size(input);

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	duckdb_vector scripts = duckdb_data_chunk_get_vector(input, 0);
	duckdb_string_t *scriptData = (duckdb_string_t *)duckdb_vector_get_data(scripts);
	uint64_t *scriptValidity = duckdb_vector_get_validity(scripts);

	LastScript lastScript(info, L);

	if (scriptValidity) {
		// if scriptValidity is defined there might be NULL values
		duckdb_vector_ensure_validity_writable(output);
		uint64_t *result_validity = duckdb_vector_get_validity(output);
		for (idx_t row = 0; row < inputSize; row++) {
			if (!scriptValidity || duckdb_validity_row_is_valid(scriptValidity, row)) {
				auto script = &scriptData[row];
				if (!lastScript.Advance(script)) {
					break;
				}

				auto error = lua_pcall(L, 0, 1, 0);
				auto resultStr = ReadLuaResponse(info, L, error);

				duckdb_vector_assign_string_element(output, row, resultStr.c_str());
			} else {
				duckdb_validity_set_row_invalid(result_validity, row);
			}
		}
	} else {
		// no NULL values - iterate and do the operation directly
		for (idx_t row = 0; row < inputSize; row++) {
			auto script = &scriptData[row];
			if (!lastScript.Advance(script)) {
				break;
			}

			auto error = lua_pcall(L, 0, 1, 0);
			auto resultStr = ReadLuaResponse(info, L, error);

			duckdb_vector_assign_string_element(output, row, resultStr.c_str());
		}
	}

	lua_close(L);
}

void LuaScalarVarcharFun(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t inputSize = duckdb_data_chunk_get_size(input);

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	duckdb_vector scripts = duckdb_data_chunk_get_vector(input, 0);
	duckdb_string_t *scriptData = (duckdb_string_t *)duckdb_vector_get_data(scripts);
	uint64_t *scriptValidity = duckdb_vector_get_validity(scripts);
	duckdb_vector args = duckdb_data_chunk_get_vector(input, 1);
	duckdb_string_t *argsData = (duckdb_string_t *)duckdb_vector_get_data(args);
	uint64_t *argsValidity = duckdb_vector_get_validity(args);

	duckdb_string_t *contextVarData = nullptr;
	uint64_t *contextVarValidity = nullptr;
	if (duckdb_data_chunk_get_column_count(input) >= 3) {
		duckdb_vector contextVar = duckdb_data_chunk_get_vector(input, 2);
		contextVarData = (duckdb_string_t *)duckdb_vector_get_data(contextVar);
		contextVarValidity = duckdb_vector_get_validity(contextVar);
	}

	LastScript lastScript(info, L);

	uint64_t *result_validity = nullptr;
	if (scriptValidity) {
		duckdb_vector_ensure_validity_writable(output);
		result_validity = duckdb_vector_get_validity(output);
	}
	for (idx_t row = 0; row < inputSize; row++) {
		if (!scriptValidity || duckdb_validity_row_is_valid(scriptValidity, row)) {
			if (!argsValidity || duckdb_validity_row_is_valid(argsValidity, row)) {
				auto arg = &argsData[row];
				lua_pushlstring(L, duckdb_string_t_data(arg), duckdb_string_t_length(*arg));
			} else {
				lua_pushnil(L);
			}
			auto contextVarName = std::string("context");
			if (contextVarData && (!contextVarValidity || duckdb_validity_row_is_valid(contextVarValidity, row))) {
				auto context = &contextVarData[row];
				contextVarName = std::string(duckdb_string_t_data(context), duckdb_string_t_length(*context));
			}
			lua_setglobal(L, contextVarName.c_str());

			auto script = &scriptData[row];
			if (!lastScript.Advance(script)) {
				break;
			}
			auto error = lua_pcall(L, 0, 1, 0);
			auto resultStr = ReadLuaResponse(info, L, error);

			duckdb_vector_assign_string_element(output, row, resultStr.c_str());

			// Reset context var
			lua_pushnil(L);
			lua_setglobal(L, contextVarName.c_str());
		} else {
			assert(result_validity);
			duckdb_validity_set_row_invalid(result_validity, row);
		}
	}

	lua_close(L);
}

void LuaScalarJsonFun(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t inputSize = duckdb_data_chunk_get_size(input);

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	std::string resultStr;
	auto jsonError =
	    luaL_loadbuffer(L, DKJSON_SOURCE.c_str(), DKJSON_SOURCE.size(), DKJSON_BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
	if (jsonError) {
		// Should not be reachable
		assert(false);
		ReadLuaResponse(info, L, jsonError);

		lua_close(L);
		return;
	}
	int dkjsonIndex = lua_gettop(L);

	duckdb_vector scripts = duckdb_data_chunk_get_vector(input, 0);
	duckdb_string_t *scriptData = (duckdb_string_t *)duckdb_vector_get_data(scripts);
	uint64_t *scriptValidity = duckdb_vector_get_validity(scripts);
	duckdb_vector args = duckdb_data_chunk_get_vector(input, 1);
	duckdb_string_t *argsData = (duckdb_string_t *)duckdb_vector_get_data(args);
	uint64_t *argsValidity = duckdb_vector_get_validity(args);

	duckdb_string_t *contextVarData = nullptr;
	uint64_t *contextVarValidity = nullptr;
	if (duckdb_data_chunk_get_column_count(input) >= 3) {
		duckdb_vector contextVar = duckdb_data_chunk_get_vector(input, 2);
		contextVarData = (duckdb_string_t *)duckdb_vector_get_data(contextVar);
		contextVarValidity = duckdb_vector_get_validity(contextVar);
	}

	LastScript lastScript(info, L);

	uint64_t *result_validity = nullptr;
	if (scriptValidity) {
		duckdb_vector_ensure_validity_writable(output);
		result_validity = duckdb_vector_get_validity(output);
	}
	for (idx_t row = 0; row < inputSize; row++) {
		if (!scriptValidity || duckdb_validity_row_is_valid(scriptValidity, row)) {
			auto script = &scriptData[row];
			if (!lastScript.Advance(script, false)) {
				break;
			}

			if (!argsValidity || duckdb_validity_row_is_valid(argsValidity, row)) {
				// json.decode
				lua_getfield(L, dkjsonIndex, "decode");
				auto arg = &argsData[row];
				lua_pushlstring(L, duckdb_string_t_data(arg), duckdb_string_t_length(*arg));
				// value, pos, error
				int decodeError = lua_pcall(L, 1, 3, 0);

				if (decodeError || lua_isstring(L, -1)) {
					// Only possible with malformed inputs (e.g. overflows)
					ReadLuaResponse(info, L, true);
					if (!decodeError) {
						// Should not practically be possible, as dkjson returns its own error indicator.
						assert(false);
						lua_pop(L, 2);
					}
					break;
				} else {
					lua_pop(L, 2);
				}
			} else {
				lua_pushnil(L);
			}
			auto contextVarName = std::string("context");
			if (contextVarData && (!contextVarValidity || duckdb_validity_row_is_valid(contextVarValidity, row))) {
				auto context = &contextVarData[row];
				contextVarName = std::string(duckdb_string_t_data(context), duckdb_string_t_length(*context));
			}
			lua_setglobal(L, contextVarName.c_str());

			// json.encode, set up for encoding after
			lua_getfield(L, dkjsonIndex, "encode");

			// Run the user code
			lastScript.PushScript();
			auto error = lua_pcall(L, 0, 1, 0);
			if (error) {
				resultStr = ReadLuaResponse(info, L, error);
				lua_pop(L, 1); // remove json.encode
			} else {
				auto encodeError = lua_pcall(L, 1, 1, 0);
				resultStr = ReadLuaResponse(info, L, encodeError);
			}

			duckdb_vector_assign_string_element(output, row, resultStr.c_str());

			// Reset context var
			lua_pushnil(L);
			lua_setglobal(L, contextVarName.c_str());
		} else {
			assert(result_validity);
			duckdb_validity_set_row_invalid(result_validity, row);
		}
	}

	lua_close(L);
}

template <typename T>
void LuaScalarNumericFun(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	constexpr auto IsBool = std::is_same<T, bool>::value;
	constexpr auto IsFloat =
	    std::is_same<T, float>::value || std::is_same<T, double>::value || std::is_same<T, uint64_t>::value;
	static_assert(!(IsBool && IsFloat), "LuaScalarNumericFun template invalid");

	idx_t inputSize = duckdb_data_chunk_get_size(input);

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	duckdb_vector scripts = duckdb_data_chunk_get_vector(input, 0);
	duckdb_string_t *scriptData = (duckdb_string_t *)duckdb_vector_get_data(scripts);
	uint64_t *scriptValidity = duckdb_vector_get_validity(scripts);
	duckdb_vector args = duckdb_data_chunk_get_vector(input, 1);
	T *argsData = (T *)duckdb_vector_get_data(args);
	uint64_t *argsValidity = duckdb_vector_get_validity(args);

	duckdb_string_t *contextVarData = nullptr;
	uint64_t *contextVarValidity = nullptr;
	if (duckdb_data_chunk_get_column_count(input) >= 3) {
		duckdb_vector contextVar = duckdb_data_chunk_get_vector(input, 2);
		contextVarData = (duckdb_string_t *)duckdb_vector_get_data(contextVar);
		contextVarValidity = duckdb_vector_get_validity(contextVar);
	}

	LastScript lastScript(info, L);

	uint64_t *result_validity = nullptr;
	if (scriptValidity) {
		duckdb_vector_ensure_validity_writable(output);
		result_validity = duckdb_vector_get_validity(output);
	}
	for (idx_t row = 0; row < inputSize; row++) {
		if (!scriptValidity || duckdb_validity_row_is_valid(scriptValidity, row)) {
			if (!argsValidity || duckdb_validity_row_is_valid(argsValidity, row)) {
				auto arg = argsData[row];
				if (IsBool) {
					lua_pushboolean(L, arg);
				} else if (IsFloat) {
					lua_pushnumber(L, arg);
				} else {
					lua_pushinteger(L, arg);
				}
			} else {
				lua_pushnil(L);
			}
			auto contextVarName = std::string("context");
			if (contextVarData && (!contextVarValidity || duckdb_validity_row_is_valid(contextVarValidity, row))) {
				auto context = &contextVarData[row];
				contextVarName = std::string(duckdb_string_t_data(context), duckdb_string_t_length(*context));
			}
			lua_setglobal(L, contextVarName.c_str());

			auto script = &scriptData[row];
			if (!lastScript.Advance(script)) {
				break;
			}
			auto error = lua_pcall(L, 0, 1, 0);
			auto resultStr = ReadLuaResponse(info, L, error);

			duckdb_vector_assign_string_element(output, row, resultStr.c_str());

			// Reset context var
			lua_pushnil(L);
			lua_setglobal(L, contextVarName.c_str());
		} else {
			assert(result_validity);
			duckdb_validity_set_row_invalid(result_validity, row);
		}
	}

	lua_close(L);
}

// lua(script: VARCHAR): VARCHAR
static void RegisterLuaScalarFunction(duckdb_scalar_function_set functionSet) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "lua");
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_scalar_function_set_return_type(function, type);
	duckdb_destroy_logical_type(&type);
	duckdb_scalar_function_set_function(function, LuaScalarFun);
	duckdb_scalar_function_set_volatile(function);
	duckdb_scalar_function_set_special_handling(function);
	duckdb_add_scalar_function_to_set(functionSet, function);
	duckdb_destroy_scalar_function(&function);
}

// lua(script: VARCHAR, stringArgument: VARCHAR): VARCHAR
template <bool WithContext>
static void RegisterLuaScalarVarcharFunction(duckdb_scalar_function_set functionSet) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "lua");
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_scalar_function_add_parameter(function, type);
	if (WithContext) {
		duckdb_scalar_function_add_parameter(function, type);
	}
	duckdb_scalar_function_set_return_type(function, type);
	duckdb_destroy_logical_type(&type);
	duckdb_scalar_function_set_function(function, LuaScalarVarcharFun);
	duckdb_scalar_function_set_volatile(function);
	duckdb_scalar_function_set_special_handling(function);
	duckdb_add_scalar_function_to_set(functionSet, function);
	duckdb_destroy_scalar_function(&function);
}

// lua(script: VARCHAR, jsonArgument: JSON): JSON
template <bool WithContext>
static void RegisterLuaScalarJsonFunction(duckdb_scalar_function_set functionSet) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "lua_json");
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_logical_type typeJson = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_logical_type_set_alias(typeJson, "JSON");
	duckdb_scalar_function_add_parameter(function, typeJson);
	if (WithContext) {
		duckdb_scalar_function_add_parameter(function, type);
	}
	duckdb_scalar_function_set_return_type(function, typeJson);
	duckdb_destroy_logical_type(&type);
	duckdb_destroy_logical_type(&typeJson);
	duckdb_scalar_function_set_function(function, LuaScalarJsonFun);
	duckdb_scalar_function_set_volatile(function);
	duckdb_scalar_function_set_special_handling(function);
	duckdb_add_scalar_function_to_set(functionSet, function);
	duckdb_destroy_scalar_function(&function);
}

// lua(script: VARCHAR, arg: T): VARCHAR
template <typename T, bool WithContext>
static void RegisterLuaScalarNumericFunction(duckdb_scalar_function_set functionSet, duckdb_type argTypeId) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "lua");
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_logical_type argType = duckdb_create_logical_type(argTypeId);
	duckdb_scalar_function_add_parameter(function, argType);
	if (WithContext) {
		duckdb_scalar_function_add_parameter(function, type);
	}
	duckdb_scalar_function_set_return_type(function, type);
	duckdb_destroy_logical_type(&type);
	duckdb_destroy_logical_type(&argType);
	duckdb_scalar_function_set_function(function, LuaScalarNumericFun<T>);
	duckdb_scalar_function_set_volatile(function);
	duckdb_scalar_function_set_special_handling(function);
	duckdb_add_scalar_function_to_set(functionSet, function);
	duckdb_destroy_scalar_function(&function);
}

void RegisterLuaFunctions(duckdb_connection connection) {
	auto luaFunctionSet = duckdb_create_scalar_function_set("lua");
	auto luaJsonFunctionSet = duckdb_create_scalar_function_set("lua_json");

	RegisterLuaScalarFunction(luaFunctionSet);
	RegisterLuaScalarVarcharFunction<false>(luaFunctionSet);
	RegisterLuaScalarJsonFunction<false>(luaJsonFunctionSet);
	RegisterLuaScalarNumericFunction<float, false>(luaFunctionSet, DUCKDB_TYPE_FLOAT);
	RegisterLuaScalarNumericFunction<double, false>(luaFunctionSet, DUCKDB_TYPE_DOUBLE);
	RegisterLuaScalarNumericFunction<int8_t, false>(luaFunctionSet, DUCKDB_TYPE_TINYINT);
	RegisterLuaScalarNumericFunction<uint8_t, false>(luaFunctionSet, DUCKDB_TYPE_UTINYINT);
	RegisterLuaScalarNumericFunction<int16_t, false>(luaFunctionSet, DUCKDB_TYPE_SMALLINT);
	RegisterLuaScalarNumericFunction<uint16_t, false>(luaFunctionSet, DUCKDB_TYPE_USMALLINT);
	RegisterLuaScalarNumericFunction<int32_t, false>(luaFunctionSet, DUCKDB_TYPE_INTEGER);
	RegisterLuaScalarNumericFunction<uint32_t, false>(luaFunctionSet, DUCKDB_TYPE_UINTEGER);
	RegisterLuaScalarNumericFunction<int64_t, false>(luaFunctionSet, DUCKDB_TYPE_BIGINT);
	RegisterLuaScalarNumericFunction<uint64_t, false>(luaFunctionSet, DUCKDB_TYPE_UBIGINT);
	RegisterLuaScalarNumericFunction<bool, false>(luaFunctionSet, DUCKDB_TYPE_BOOLEAN);

	RegisterLuaScalarVarcharFunction<true>(luaFunctionSet);
	RegisterLuaScalarJsonFunction<true>(luaJsonFunctionSet);
	RegisterLuaScalarNumericFunction<float, true>(luaFunctionSet, DUCKDB_TYPE_FLOAT);
	RegisterLuaScalarNumericFunction<double, true>(luaFunctionSet, DUCKDB_TYPE_DOUBLE);
	RegisterLuaScalarNumericFunction<int8_t, true>(luaFunctionSet, DUCKDB_TYPE_TINYINT);
	RegisterLuaScalarNumericFunction<uint8_t, true>(luaFunctionSet, DUCKDB_TYPE_UTINYINT);
	RegisterLuaScalarNumericFunction<int16_t, true>(luaFunctionSet, DUCKDB_TYPE_SMALLINT);
	RegisterLuaScalarNumericFunction<uint16_t, true>(luaFunctionSet, DUCKDB_TYPE_USMALLINT);
	RegisterLuaScalarNumericFunction<int32_t, true>(luaFunctionSet, DUCKDB_TYPE_INTEGER);
	RegisterLuaScalarNumericFunction<uint32_t, true>(luaFunctionSet, DUCKDB_TYPE_UINTEGER);
	RegisterLuaScalarNumericFunction<int64_t, true>(luaFunctionSet, DUCKDB_TYPE_BIGINT);
	RegisterLuaScalarNumericFunction<uint64_t, true>(luaFunctionSet, DUCKDB_TYPE_UBIGINT);
	RegisterLuaScalarNumericFunction<bool, true>(luaFunctionSet, DUCKDB_TYPE_BOOLEAN);

	duckdb_register_scalar_function_set(connection, luaFunctionSet);
	duckdb_destroy_scalar_function_set(&luaFunctionSet);
	duckdb_register_scalar_function_set(connection, luaJsonFunctionSet);
	duckdb_destroy_scalar_function_set(&luaJsonFunctionSet);
}

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info,
                            struct duckdb_extension_access *access) {
	// TODO: Set extension description
	// loader.SetDescription("Lua embedded scripting language, " LUA_RELEASE);
	// TODO: Set extension version
	// EXT_VERSION_LUA, if defined

	RegisterLuaFunctions(connection);

	// Return true to indicate succesful initialization
	return true;
}
