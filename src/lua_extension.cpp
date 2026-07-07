#define DUCKDB_EXTENSION_MAIN

#include "lua_extension.hpp"
#include "dkjson.hpp"
#include <string.h>

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}

const auto BUFFER_NAME = "line";
const auto CONTEXT_OPTION_NAME = "lua_context_name";

inline std::string ReadLuaResponse(lua_State *L, bool error) {
	std::string resultStr;
	if (error) {
		resultStr = lua_tostring(L, -1);
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

DUCKDB_EXTENSION_EXTERN inline void LuaScalarFun(duckdb_function_info info, duckdb_data_chunk input,
                                                 duckdb_vector output) {
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
	//	UnifiedVectorFormat scriptData;
	//	args.data[0].ToUnifiedFormat(args.size(), scriptData);
	//	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);
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
				// either a or b is NULL - set the result row to NULL
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
}

// inline void LuaScalarJsonFun(DataChunk &args, ExpressionState &state, Vector &result) {
//	Value contextVarNameValue = Value(LogicalType::VARCHAR);
//	state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
//	auto contextVarName = contextVarNameValue.GetValue<string>();
//
//	lua_State *L = luaL_newstate();
//	luaL_openlibs(L);
//
//	lua_pushnil(L);
//	lua_setglobal(L, contextVarName.c_str());
//
//	UnifiedVectorFormat scriptData;
//	args.data[0].ToUnifiedFormat(args.size(), scriptData);
//	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);
//
//	UnifiedVectorFormat argData;
//	args.data[1].ToUnifiedFormat(args.size(), argData);
//	auto argDataPtr = UnifiedVectorFormat::GetData<string_t>(argData);
//
//	result.SetVectorType(VectorType::FLAT_VECTOR);
//	auto result_data = FlatVector::GetData<string_t>(result);
//
//	std::string resultStr;
//	auto jsonError =
//	    luaL_loadbuffer(L, DKJSON_SOURCE.c_str(), DKJSON_SOURCE.size(), DKJSON_BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
//	if (jsonError) {
//		resultStr = ReadLuaResponse(L, jsonError);
//	}
//
//	for (idx_t i = 0; i < args.size(); i++) {
//		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i))) {
//			result.SetValue(i, Value(LogicalType::JSON()));
//			continue;
//		}
//		if (!jsonError) {
//			auto script = scriptDataPtr[scriptData.sel->get_index(i)];
//			int decodeError = 0;
//			if (argData.validity.RowIsValid(argData.sel->get_index(i))) {
//				// json.decode
//				lua_getfield(L, -1, "decode");
//				auto data = argDataPtr[argData.sel->get_index(i)];
//				lua_pushlstring(L, data.GetData(), data.GetSize());
//				decodeError = lua_pcall(L, 1, 1, 0);
//			} else {
//				lua_pushnil(L);
//			}
//			if (decodeError) {
//				resultStr = ReadLuaResponse(L, decodeError);
//			} else {
//				lua_setglobal(L, contextVarName.c_str());
//				// json.encode, set up for encoding after
//				lua_getfield(L, -1, "encode");
//
//				// Run the user code
//				auto error =
//				    luaL_loadbuffer(L, script.GetData(), script.GetSize(), BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
//				if (error) {
//					resultStr = ReadLuaResponse(L, error);
//					lua_pop(L, 1); // remove json.encode
//				} else {
//					auto encodeError = lua_pcall(L, 1, 1, 0);
//					resultStr = ReadLuaResponse(L, encodeError);
//				}
//			}
//		}
//
//		auto resultDuckdbStr = string_t(strdup(resultStr.c_str()), resultStr.size());
//
//		result_data[i] = StringVector::AddString(result, resultDuckdbStr);
//	}
//
//	lua_close(L);
//	result.Verify(args.size());
//}
//
// inline void LuaScalarVarcharFun(DataChunk &args, ExpressionState &state, Vector &result) {
//	Value contextVarNameValue = Value(LogicalType::VARCHAR);
//	state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
//	auto contextVarName = contextVarNameValue.GetValue<string>();
//
//	lua_State *L = luaL_newstate();
//	luaL_openlibs(L);
//
//	UnifiedVectorFormat scriptData;
//	args.data[0].ToUnifiedFormat(args.size(), scriptData);
//	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);
//
//	UnifiedVectorFormat argData;
//	args.data[1].ToUnifiedFormat(args.size(), argData);
//	auto argDataPtr = UnifiedVectorFormat::GetData<string_t>(argData);
//
//	result.SetVectorType(VectorType::FLAT_VECTOR);
//	auto result_data = FlatVector::GetData<string_t>(result);
//	for (idx_t i = 0; i < args.size(); i++) {
//		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i))) {
//			result.SetValue(i, Value(LogicalType::VARCHAR));
//			continue;
//		}
//
//		auto script = scriptDataPtr[scriptData.sel->get_index(i)];
//
//		if (argData.validity.RowIsValid(argData.sel->get_index(i))) {
//			auto data = argDataPtr[argData.sel->get_index(i)];
//			lua_pushlstring(L, data.GetData(), data.GetSize());
//		} else {
//			lua_pushnil(L);
//		}
//		lua_setglobal(L, contextVarName.c_str());
//
//		// Run the user code
//		auto error = luaL_loadbuffer(L, script.GetData(), script.GetSize(), BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
//		auto resultStr = ReadLuaResponse(L, error);
//		auto resultDuckdbStr = string_t(strdup(resultStr.c_str()), resultStr.size());
//
//		result_data[i] = StringVector::AddString(result, resultDuckdbStr);
//	}
//
//	lua_close(L);
//	result.Verify(args.size());
//}
//
// template <typename T, bool IsInteger, bool IsBool>
// inline void LuaScalarNumericFun(DataChunk &args, ExpressionState &state, Vector &result) {
//	Value contextVarNameValue = Value(LogicalType::VARCHAR);
//	state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
//	auto contextVarName = contextVarNameValue.GetValue<string>();
//
//	lua_State *L = luaL_newstate();
//	luaL_openlibs(L);
//
//	UnifiedVectorFormat scriptData;
//	args.data[0].ToUnifiedFormat(args.size(), scriptData);
//	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);
//
//	UnifiedVectorFormat argData;
//	args.data[1].ToUnifiedFormat(args.size(), argData);
//	auto argDataPtr = UnifiedVectorFormat::GetData<T>(argData);
//
//	result.SetVectorType(VectorType::FLAT_VECTOR);
//	auto result_data = FlatVector::GetData<string_t>(result);
//	for (idx_t i = 0; i < args.size(); i++) {
//		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i))) {
//			result.SetValue(i, Value(LogicalType::VARCHAR));
//			continue;
//		}
//
//		auto script = scriptDataPtr[scriptData.sel->get_index(i)];
//
//		if (argData.validity.RowIsValid(argData.sel->get_index(i))) {
//			auto data = argDataPtr[argData.sel->get_index(i)];
//			static_assert(!(IsBool && IsInteger), "LuaScalarNumericFun template invalid");
//			if (IsBool) {
//				lua_pushboolean(L, data);
//			} else if (IsInteger) {
//				lua_pushinteger(L, data);
//			} else {
//				lua_pushnumber(L, data);
//			}
//		} else {
//			lua_pushnil(L);
//		}
//		lua_setglobal(L, contextVarName.c_str());
//
//		// Run the user code
//		auto error = luaL_loadbuffer(L, script.GetData(), script.GetSize(), BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
//		auto resultStr = ReadLuaResponse(L, error);
//		auto resultDuckdbStr = string_t(strdup(resultStr.c_str()), resultStr.size());
//
//		result_data[i] = StringVector::AddString(result, resultDuckdbStr);
//	}
//
//	lua_close(L);
//	result.Verify(args.size());
//}

// static void LoadInternal(ExtensionLoader &loader) {
//	loader.SetDescription("Lua embedded scripting language, " LUA_RELEASE);
//
//	ScalarFunctionSet lua_scalar_functions("lua");
//
//	// We don't know if the user script calls e.g. math.random(), so deoptimize
//	auto stability = FunctionStability::VOLATILE;
//
//	auto lua_scalar_function =
//	    ScalarFunction("lua", {LogicalType::VARCHAR}, LogicalType::VARCHAR, LuaScalarFun, nullptr, nullptr, nullptr,
//	                   nullptr, LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING);
//	lua_scalar_functions.AddFunction(lua_scalar_function);
//
//	auto lua_scalar_function_varchar =
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, LuaScalarVarcharFun,
//	                   nullptr, nullptr, nullptr, nullptr, LogicalType(LogicalTypeId::INVALID), stability,
//	                   FunctionNullHandling::SPECIAL_HANDLING);
//	lua_scalar_functions.AddFunction(lua_scalar_function_varchar);
//
//	auto lua_scalar_function_json = ScalarFunction(
//	    "lua", {LogicalType::VARCHAR, LogicalType::JSON()}, LogicalType::JSON(), LuaScalarJsonFun, nullptr, nullptr,
//	    nullptr, nullptr, LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING);
//	lua_scalar_functions.AddFunction(lua_scalar_function_json);
//
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::FLOAT}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<float, false, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::DOUBLE}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<double, false, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::TINYINT}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<int8_t, true, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::UTINYINT}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<uint8_t, true, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::SMALLINT}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<int16_t, true, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::USMALLINT}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<uint16_t, true, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<int32_t, true, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::UINTEGER}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<uint32_t, true, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<int64_t, true, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::UBIGINT}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<uint64_t, false, false>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//	lua_scalar_functions.AddFunction(
//	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::BOOLEAN}, LogicalType::VARCHAR,
//	                   LuaScalarNumericFun<bool, false, true>, nullptr, nullptr, nullptr, nullptr,
//	                   LogicalType(LogicalTypeId::INVALID), stability, FunctionNullHandling::SPECIAL_HANDLING));
//
//	loader.RegisterFunction(lua_scalar_functions);
//
//	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
//	config.AddExtensionOption("lua_context_name", "Global context variable name. Default: 'context'",
//	                          LogicalType::VARCHAR, Value("context"));
//}
//
// void LuaExtension::Load(ExtensionLoader &loader) {
//	LoadInternal(loader);
//}
// std::string LuaExtension::Name() {
//	return "lua";
//}
//
// std::string LuaExtension::Version() const {
//#ifdef EXT_VERSION_LUA
//	return EXT_VERSION_LUA;
//#else
//	return "";
//#endif
//}

void RegisterLuaFunctions(duckdb_connection connection) {
	// create a scalar function
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "lua");

	// add a string parameter
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(function, type);

	// set the return type to string
	duckdb_scalar_function_set_return_type(function, type);

	duckdb_destroy_logical_type(&type);

	// set up the function
	duckdb_scalar_function_set_function(function, LuaScalarFun);

	// register and cleanup
	duckdb_register_scalar_function(connection, function);
	duckdb_destroy_scalar_function(&function);
}

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info,
                            struct duckdb_extension_access *access) {
	RegisterLuaFunctions(connection);

	// Return true to indicate succesful initialization
	return true;
}
