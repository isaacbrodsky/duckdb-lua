#define DUCKDB_EXTENSION_MAIN

#include "lua_extension.hpp"
#include "dkjson.hpp"
#include <string>
#include <type_traits>

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}

DUCKDB_EXTENSION_EXTERN

const auto BUFFER_NAME = "line";
const auto CONTEXT_OPTION_NAME = "lua_context_name";

inline static std::string ReadLuaResponse(lua_State *L, bool error) {
	std::string resultStr;
	if (error) {
		if (lua_isstring(L, -1)) {
			resultStr = lua_tostring(L, -1);
		} else {
			resultStr = std::string("Unknown Error: ") + lua_typename(L, -1);
		}
		lua_pop(L, 1);
	} else {
		if (lua_isnoneornil(L, -1)) {
			resultStr = "nil";
			lua_pop(L, 1);
		} else if (lua_isstring(L, -1)) {
			resultStr = lua_tostring(L, -1);
			lua_pop(L, 1);
		} else if (lua_isinteger(L, -1)) {
			resultStr = std::to_string(lua_tointeger(L, -1));
			lua_pop(L, 1);
		} else if (lua_isnumber(L, -1)) {
			resultStr = std::to_string(lua_tonumber(L, -1));
			lua_pop(L, 1);
		} else if (lua_isboolean(L, -1)) {
			resultStr = lua_toboolean(L, -1) ? "true" : "false";
			lua_pop(L, 1);
		} else {
			resultStr = std::string("Unknown type: ") + lua_typename(L, -1);
			lua_pop(L, 1);
		}
	}
	return resultStr;
}

void LuaScalarFun(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t inputSize = duckdb_data_chunk_get_size(input);

	// Unsupported: settings
	// Value contextVarNameValue = Value(LogicalType::VARCHAR);
	// state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = std::string("context"); // contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	lua_pushnil(L);
	lua_setglobal(L, contextVarName.c_str());

	duckdb_vector scripts = duckdb_data_chunk_get_vector(input, 0);
	duckdb_string_t *scriptData = (duckdb_string_t *)duckdb_vector_get_data(scripts);
	uint64_t *scriptValidity = duckdb_vector_get_validity(scripts);

	if (scriptValidity) {
		// if scriptValidity is defined there might be NULL values
		duckdb_vector_ensure_validity_writable(output);
		uint64_t *result_validity = duckdb_vector_get_validity(output);
		for (idx_t row = 0; row < inputSize; row++) {
			if (duckdb_validity_row_is_valid(scriptValidity, row)) {
				auto script = &scriptData[row];
				auto error =
				    luaL_loadbuffer(L, duckdb_string_t_data(script), duckdb_string_t_length(*script), BUFFER_NAME) ||
				    lua_pcall(L, 0, 1, 0);
				auto resultStr = ReadLuaResponse(L, error);

				duckdb_vector_assign_string_element(output, row, resultStr.c_str());
			} else {
				duckdb_validity_set_row_invalid(result_validity, row);
			}
		}
	} else {
		// no NULL values - iterate and do the operation directly
		for (idx_t row = 0; row < inputSize; row++) {
			auto script = &scriptData[row];
			auto error =
			    luaL_loadbuffer(L, duckdb_string_t_data(script), duckdb_string_t_length(*script), BUFFER_NAME) ||
			    lua_pcall(L, 0, 1, 0);
			auto resultStr = ReadLuaResponse(L, error);

			duckdb_vector_assign_string_element(output, row, resultStr.c_str());
		}
	}

	lua_close(L);
}

void LuaScalarVarcharFun(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t inputSize = duckdb_data_chunk_get_size(input);

	// Unsupported: settings
	// Value contextVarNameValue = Value(LogicalType::VARCHAR);
	// state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = std::string("context"); // contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	duckdb_vector scripts = duckdb_data_chunk_get_vector(input, 0);
	duckdb_string_t *scriptData = (duckdb_string_t *)duckdb_vector_get_data(scripts);
	uint64_t *scriptValidity = duckdb_vector_get_validity(scripts);
	duckdb_vector args = duckdb_data_chunk_get_vector(input, 1);
	duckdb_string_t *argsData = (duckdb_string_t *)duckdb_vector_get_data(args);
	uint64_t *argsValidity = duckdb_vector_get_validity(args);

	if (scriptValidity) {
		// if scriptValidity is defined there might be NULL values
		duckdb_vector_ensure_validity_writable(output);
		uint64_t *result_validity = duckdb_vector_get_validity(output);
		for (idx_t row = 0; row < inputSize; row++) {
			if (duckdb_validity_row_is_valid(scriptValidity, row)) {
				if (!argsValidity || duckdb_validity_row_is_valid(argsValidity, row)) {
					auto arg = &argsData[row];
					lua_pushlstring(L, duckdb_string_t_data(arg), duckdb_string_t_length(*arg));
				} else {
					lua_pushnil(L);
				}
				lua_setglobal(L, contextVarName.c_str());

				auto script = &scriptData[row];
				auto error =
				    luaL_loadbuffer(L, duckdb_string_t_data(script), duckdb_string_t_length(*script), BUFFER_NAME) ||
				    lua_pcall(L, 0, 1, 0);
				auto resultStr = ReadLuaResponse(L, error);

				duckdb_vector_assign_string_element(output, row, resultStr.c_str());
			} else {
				duckdb_validity_set_row_invalid(result_validity, row);
			}
		}
	} else {
		// no NULL values - iterate and do the operation directly
		for (idx_t row = 0; row < inputSize; row++) {
			if (!argsValidity || duckdb_validity_row_is_valid(argsValidity, row)) {
				auto arg = &argsData[row];
				lua_pushlstring(L, duckdb_string_t_data(arg), duckdb_string_t_length(*arg));
			} else {
				lua_pushnil(L);
			}
			lua_setglobal(L, contextVarName.c_str());

			auto script = &scriptData[row];
			auto error =
			    luaL_loadbuffer(L, duckdb_string_t_data(script), duckdb_string_t_length(*script), BUFFER_NAME) ||
			    lua_pcall(L, 0, 1, 0);
			auto resultStr = ReadLuaResponse(L, error);

			duckdb_vector_assign_string_element(output, row, resultStr.c_str());
		}
	}

	lua_close(L);
}

void LuaScalarJsonFun(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t inputSize = duckdb_data_chunk_get_size(input);

	// Unsupported: settings
	// Value contextVarNameValue = Value(LogicalType::VARCHAR);
	// state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = std::string("context"); // contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	lua_pushnil(L);
	lua_setglobal(L, contextVarName.c_str());

	std::string resultStr;
	auto jsonError =
	    luaL_loadbuffer(L, DKJSON_SOURCE.c_str(), DKJSON_SOURCE.size(), DKJSON_BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
	if (jsonError) {
		resultStr = ReadLuaResponse(L, jsonError);
		for (idx_t row = 0; row < inputSize; row++) {
			duckdb_vector_assign_string_element(output, row, resultStr.c_str());
		}

		lua_close(L);
		return;
	}

	duckdb_vector scripts = duckdb_data_chunk_get_vector(input, 0);
	duckdb_string_t *scriptData = (duckdb_string_t *)duckdb_vector_get_data(scripts);
	uint64_t *scriptValidity = duckdb_vector_get_validity(scripts);
	duckdb_vector args = duckdb_data_chunk_get_vector(input, 1);
	duckdb_string_t *argsData = (duckdb_string_t *)duckdb_vector_get_data(args);
	uint64_t *argsValidity = duckdb_vector_get_validity(args);

	if (scriptValidity) {
		// if scriptValidity is defined there might be NULL values
		duckdb_vector_ensure_validity_writable(output);
		uint64_t *result_validity = duckdb_vector_get_validity(output);
		for (idx_t row = 0; row < inputSize; row++) {
			if (duckdb_validity_row_is_valid(scriptValidity, row)) {
				if (!argsValidity || duckdb_validity_row_is_valid(argsValidity, row)) {
					// json.decode
					lua_getfield(L, -1, "decode");
					auto arg = &argsData[row];
					lua_pushlstring(L, duckdb_string_t_data(arg), duckdb_string_t_length(*arg));
					int decodeError = lua_pcall(L, 1, 1, 0);

					if (decodeError) {
						resultStr = ReadLuaResponse(L, decodeError);
						duckdb_vector_assign_string_element(output, row, resultStr.c_str());
						continue;
					}
				} else {
					lua_pushnil(L);
				}
				lua_setglobal(L, contextVarName.c_str());

				// json.encode, set up for encoding after
				lua_getfield(L, -1, "encode");

				// Run the user code
				auto script = &scriptData[row];
				auto error =
				    luaL_loadbuffer(L, duckdb_string_t_data(script), duckdb_string_t_length(*script), BUFFER_NAME) ||
				    lua_pcall(L, 0, 1, 0);
				if (error) {
					resultStr = ReadLuaResponse(L, error);
					lua_pop(L, 1); // remove json.encode
				} else {
					auto encodeError = lua_pcall(L, 1, 1, 0);
					resultStr = ReadLuaResponse(L, encodeError);
				}

				duckdb_vector_assign_string_element(output, row, resultStr.c_str());
			} else {
				duckdb_validity_set_row_invalid(result_validity, row);
			}
		}
	} else {
		// no NULL values - iterate and do the operation directly
		for (idx_t row = 0; row < inputSize; row++) {
			if (!argsValidity || duckdb_validity_row_is_valid(argsValidity, row)) {
				// json.decode
				lua_getfield(L, -1, "decode");
				auto arg = &argsData[row];
				lua_pushlstring(L, duckdb_string_t_data(arg), duckdb_string_t_length(*arg));
				int decodeError = lua_pcall(L, 1, 1, 0);

				if (decodeError) {
					resultStr = ReadLuaResponse(L, decodeError);
					duckdb_vector_assign_string_element(output, row, resultStr.c_str());
					continue;
				}
			} else {
				lua_pushnil(L);
			}
			lua_setglobal(L, contextVarName.c_str());

			// json.encode, set up for encoding after
			lua_getfield(L, -1, "encode");

			// Run the user code
			auto script = &scriptData[row];
			auto error =
			    luaL_loadbuffer(L, duckdb_string_t_data(script), duckdb_string_t_length(*script), BUFFER_NAME) ||
			    lua_pcall(L, 0, 1, 0);
			if (error) {
				resultStr = ReadLuaResponse(L, error);
				lua_pop(L, 1); // remove json.encode
			} else {
				auto encodeError = lua_pcall(L, 1, 1, 0);
				resultStr = ReadLuaResponse(L, encodeError);
			}

			duckdb_vector_assign_string_element(output, row, resultStr.c_str());
		}
	}

	lua_close(L);
}

template <typename T>
void LuaScalarNumericFun(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	constexpr auto IsBool = std::is_same<T, bool>::value;
	constexpr auto IsFloat = std::is_same<T, float>::value || std::is_same<T, double>::value;
	static_assert(!(IsBool && IsFloat), "LuaScalarNumericFun template invalid");

	idx_t inputSize = duckdb_data_chunk_get_size(input);

	// Unsupported: settings
	// Value contextVarNameValue = Value(LogicalType::VARCHAR);
	// state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = std::string("context"); // contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	duckdb_vector scripts = duckdb_data_chunk_get_vector(input, 0);
	duckdb_string_t *scriptData = (duckdb_string_t *)duckdb_vector_get_data(scripts);
	uint64_t *scriptValidity = duckdb_vector_get_validity(scripts);
	duckdb_vector args = duckdb_data_chunk_get_vector(input, 1);
	T *argsData = (T *)duckdb_vector_get_data(args);
	uint64_t *argsValidity = duckdb_vector_get_validity(args);

	if (scriptValidity) {
		// if scriptValidity is defined there might be NULL values
		duckdb_vector_ensure_validity_writable(output);
		uint64_t *result_validity = duckdb_vector_get_validity(output);
		for (idx_t row = 0; row < inputSize; row++) {
			if (duckdb_validity_row_is_valid(scriptValidity, row)) {
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
				lua_setglobal(L, contextVarName.c_str());

				auto script = &scriptData[row];
				auto error =
				    luaL_loadbuffer(L, duckdb_string_t_data(script), duckdb_string_t_length(*script), BUFFER_NAME) ||
				    lua_pcall(L, 0, 1, 0);
				auto resultStr = ReadLuaResponse(L, error);

				duckdb_vector_assign_string_element(output, row, resultStr.c_str());
			} else {
				duckdb_validity_set_row_invalid(result_validity, row);
			}
		}
	} else {
		// no NULL values - iterate and do the operation directly
		for (idx_t row = 0; row < inputSize; row++) {
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
			lua_setglobal(L, contextVarName.c_str());

			auto script = &scriptData[row];
			auto error =
			    luaL_loadbuffer(L, duckdb_string_t_data(script), duckdb_string_t_length(*script), BUFFER_NAME) ||
			    lua_pcall(L, 0, 1, 0);
			auto resultStr = ReadLuaResponse(L, error);

			duckdb_vector_assign_string_element(output, row, resultStr.c_str());
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
static void RegisterLuaScalarVarcharFunction(duckdb_scalar_function_set functionSet) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "lua");
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_scalar_function_set_return_type(function, type);
	duckdb_destroy_logical_type(&type);
	duckdb_scalar_function_set_function(function, LuaScalarVarcharFun);
	duckdb_scalar_function_set_volatile(function);
	duckdb_scalar_function_set_special_handling(function);
	duckdb_add_scalar_function_to_set(functionSet, function);
	duckdb_destroy_scalar_function(&function);
}

// lua(script: VARCHAR, jsonArgument: JSON): JSON
static void RegisterLuaScalarJsonFunction(duckdb_scalar_function_set functionSet) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "lua");
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_logical_type typeJson = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_logical_type_set_alias(typeJson, "JSON");
	duckdb_scalar_function_add_parameter(function, typeJson);
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
template <typename T>
static void RegisterLuaScalarNumericFunction(duckdb_scalar_function_set functionSet, duckdb_type argTypeId) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "lua");
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_logical_type argType = duckdb_create_logical_type(argTypeId);
	duckdb_scalar_function_add_parameter(function, argType);
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

	RegisterLuaScalarFunction(luaFunctionSet);
	RegisterLuaScalarVarcharFunction(luaFunctionSet);
	RegisterLuaScalarJsonFunction(luaFunctionSet);
	RegisterLuaScalarNumericFunction<float>(luaFunctionSet, DUCKDB_TYPE_FLOAT);
	RegisterLuaScalarNumericFunction<double>(luaFunctionSet, DUCKDB_TYPE_DOUBLE);
	RegisterLuaScalarNumericFunction<int8_t>(luaFunctionSet, DUCKDB_TYPE_TINYINT);
	RegisterLuaScalarNumericFunction<uint8_t>(luaFunctionSet, DUCKDB_TYPE_UTINYINT);
	RegisterLuaScalarNumericFunction<int16_t>(luaFunctionSet, DUCKDB_TYPE_SMALLINT);
	RegisterLuaScalarNumericFunction<uint16_t>(luaFunctionSet, DUCKDB_TYPE_USMALLINT);
	RegisterLuaScalarNumericFunction<int32_t>(luaFunctionSet, DUCKDB_TYPE_INTEGER);
	RegisterLuaScalarNumericFunction<uint32_t>(luaFunctionSet, DUCKDB_TYPE_UINTEGER);
	RegisterLuaScalarNumericFunction<int64_t>(luaFunctionSet, DUCKDB_TYPE_BIGINT);
	RegisterLuaScalarNumericFunction<uint64_t>(luaFunctionSet, DUCKDB_TYPE_UBIGINT);
	RegisterLuaScalarNumericFunction<bool>(luaFunctionSet, DUCKDB_TYPE_BOOLEAN);

	duckdb_register_scalar_function_set(connection, luaFunctionSet);
	duckdb_destroy_scalar_function_set(&luaFunctionSet);
}

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info,
                            struct duckdb_extension_access *access) {
	// TODO: Set extension description
	// loader.SetDescription("Lua embedded scripting language, " LUA_RELEASE);
	// TODO: Set extension version
	// EXT_VERSION_LUA, if defined
	// TODO: Add lua_context_name config
	// auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	// config.AddExtensionOption("lua_context_name", "Global context variable name. Default: 'context'",
	// LogicalType::VARCHAR, Value("context"));

	RegisterLuaFunctions(connection);

	// Return true to indicate succesful initialization
	return true;
}
