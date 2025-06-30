#define DUCKDB_EXTENSION_MAIN

#include "lua_extension.hpp"
#include "dkjson.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <string.h>

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}

namespace duckdb {

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
			resultStr = StringUtil::Format("%d", lua_tointeger(L, -1));
			lua_pop(L, 1);
		} else if (lua_isnumber(L, -1)) {
			resultStr = StringUtil::Format("%f", lua_tonumber(L, -1));
			lua_pop(L, 1);
		} else if (lua_isboolean(L, -1)) {
			resultStr = lua_toboolean(L, -1) ? "true" : "false";
			lua_pop(L, 1);
		} else {
			resultStr = StringUtil::Format("Unknown type: %s", lua_typename(L, -1));
			lua_pop(L, 1);
		}
	}
	return resultStr;
}

inline void LuaScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	Value contextVarNameValue = Value(LogicalType::VARCHAR);
	state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	lua_pushnil(L);
	lua_setglobal(L, contextVarName.c_str());

	UnifiedVectorFormat scriptData;
	args.data[0].ToUnifiedFormat(args.size(), scriptData);
	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i))) {
			// TODO
			result_data[i] = StringVector::AddString(result, "");
			continue;
		}

		// Run the user code
		auto script = scriptDataPtr[scriptData.sel->get_index(i)];
		auto error = luaL_loadbuffer(L, script.GetData(), script.GetSize(), BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
		auto resultStr = ReadLuaResponse(L, error);
		auto resultDuckdbStr = string_t(strdup(resultStr.c_str()), resultStr.size());

		result_data[i] = StringVector::AddString(result, resultDuckdbStr);
	}

	lua_close(L);
	result.Verify(args.size());
}

inline void LuaScalarJsonFun(DataChunk &args, ExpressionState &state, Vector &result) {
	Value contextVarNameValue = Value(LogicalType::VARCHAR);
	state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	lua_pushnil(L);
	lua_setglobal(L, contextVarName.c_str());

	UnifiedVectorFormat scriptData;
	args.data[0].ToUnifiedFormat(args.size(), scriptData);
	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);

	UnifiedVectorFormat argData;
	args.data[1].ToUnifiedFormat(args.size(), argData);
	auto argDataPtr = UnifiedVectorFormat::GetData<string_t>(argData);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<string_t>(result);

	std::string resultStr;
	auto jsonError =
	    luaL_loadbuffer(L, DKJSON_SOURCE.c_str(), DKJSON_SOURCE.size(), DKJSON_BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
	if (jsonError) {
		resultStr = ReadLuaResponse(L, jsonError);
	}

	for (idx_t i = 0; i < args.size(); i++) {
		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i)) ||
		    !argData.validity.RowIsValid(argData.sel->get_index(i))) {
			// TODO
			result_data[i] = StringVector::AddString(result, "");
			continue;
		}
		if (!jsonError) {
			// json.decode
			auto decodeField = lua_getfield(L, -1, "decode");

			auto script = scriptDataPtr[scriptData.sel->get_index(i)];
			auto data = argDataPtr[argData.sel->get_index(i)];
			lua_pushlstring(L, data.GetData(), data.GetSize());
			auto decodeError = lua_pcall(L, 1, 1, 0);
			if (decodeError) {
				resultStr = ReadLuaResponse(L, decodeError);
			} else {
				lua_setglobal(L, contextVarName.c_str());
				// json.encode, set up for encoding after
				lua_getfield(L, -1, "encode");

				// Run the user code
				auto error =
				    luaL_loadbuffer(L, script.GetData(), script.GetSize(), BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
				if (error) {
					resultStr = ReadLuaResponse(L, error);
				} else {
					auto encodeError = lua_pcall(L, 1, 1, 0);
					resultStr = ReadLuaResponse(L, encodeError);
				}
			}
		}

		auto resultDuckdbStr = string_t(strdup(resultStr.c_str()), resultStr.size());

		result_data[i] = StringVector::AddString(result, resultDuckdbStr);
	}

	lua_close(L);
	result.Verify(args.size());
}

inline void LuaScalarVarcharFun(DataChunk &args, ExpressionState &state, Vector &result) {
	Value contextVarNameValue = Value(LogicalType::VARCHAR);
	state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	UnifiedVectorFormat scriptData;
	args.data[0].ToUnifiedFormat(args.size(), scriptData);
	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);

	UnifiedVectorFormat argData;
	args.data[1].ToUnifiedFormat(args.size(), argData);
	auto argDataPtr = UnifiedVectorFormat::GetData<string_t>(argData);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i)) ||
		    !argData.validity.RowIsValid(argData.sel->get_index(i))) {
			// TODO
			result_data[i] = StringVector::AddString(result, "");
			continue;
		}

		auto script = scriptDataPtr[scriptData.sel->get_index(i)];
		auto data = argDataPtr[argData.sel->get_index(i)];
		lua_pushlstring(L, data.GetData(), data.GetSize());
		lua_setglobal(L, contextVarName.c_str());

		// Run the user code
		auto error = luaL_loadbuffer(L, script.GetData(), script.GetSize(), BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
		auto resultStr = ReadLuaResponse(L, error);
		auto resultDuckdbStr = string_t(strdup(resultStr.c_str()), resultStr.size());

		result_data[i] = StringVector::AddString(result, resultDuckdbStr);
	}

	lua_close(L);
	result.Verify(args.size());
}

template <typename T, bool IsInteger, bool IsBool>
inline void LuaScalarNumericFun(DataChunk &args, ExpressionState &state, Vector &result) {
	Value contextVarNameValue = Value(LogicalType::VARCHAR);
	state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	UnifiedVectorFormat scriptData;
	args.data[0].ToUnifiedFormat(args.size(), scriptData);
	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);

	UnifiedVectorFormat argData;
	args.data[1].ToUnifiedFormat(args.size(), argData);
	auto argDataPtr = UnifiedVectorFormat::GetData<T>(argData);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i)) ||
		    !argData.validity.RowIsValid(argData.sel->get_index(i))) {
			// TODO
			result_data[i] = StringVector::AddString(result, "");
			continue;
		}

		auto script = scriptDataPtr[scriptData.sel->get_index(i)];
		auto data = argDataPtr[argData.sel->get_index(i)];
		static_assert(!(IsBool && IsInteger), "LuaScalarNumericFun template invalid");
		if (IsBool) {
			lua_pushboolean(L, data);
		} else if (IsInteger) {
			lua_pushinteger(L, data);
		} else {
			lua_pushnumber(L, data);
		}
		lua_setglobal(L, contextVarName.c_str());

		// Run the user code
		auto error = luaL_loadbuffer(L, script.GetData(), script.GetSize(), BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
		auto resultStr = ReadLuaResponse(L, error);
		auto resultDuckdbStr = string_t(strdup(resultStr.c_str()), resultStr.size());

		result_data[i] = StringVector::AddString(result, resultDuckdbStr);
	}

	lua_close(L);
	result.Verify(args.size());
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription(StringUtil::Format("Lua embedded scripting language, %s", LUA_RELEASE));

	ScalarFunctionSet lua_scalar_functions("lua");

	auto lua_scalar_function = ScalarFunction("lua", {LogicalType::VARCHAR}, LogicalType::VARCHAR, LuaScalarFun);
	lua_scalar_functions.AddFunction(lua_scalar_function);

	auto lua_scalar_function_varchar =
	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, LuaScalarVarcharFun);
	lua_scalar_functions.AddFunction(lua_scalar_function_varchar);

	auto lua_scalar_function_json =
	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::JSON()}, LogicalType::JSON(), LuaScalarJsonFun);
	lua_scalar_functions.AddFunction(lua_scalar_function_json);

	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::FLOAT},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<float, false, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::DOUBLE},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<double, false, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::TINYINT},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<int8_t, true, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::UTINYINT},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<uint8_t, true, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::SMALLINT},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<int16_t, true, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::USMALLINT},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<uint16_t, true, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::INTEGER},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<int32_t, true, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::UINTEGER},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<uint32_t, true, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::BIGINT},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<int64_t, true, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::UBIGINT},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<uint64_t, false, false>));
	lua_scalar_functions.AddFunction(ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                                                LogicalType::VARCHAR, LuaScalarNumericFun<bool, false, true>));

	loader.RegisterFunction(lua_scalar_functions);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("lua_context_name", "Global context variable name. Default: 'context'",
	                          LogicalType::VARCHAR, Value("context"));
}

void LuaExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string LuaExtension::Name() {
	return "lua";
}

std::string LuaExtension::Version() const {
#ifdef EXT_VERSION_LUA
	return EXT_VERSION_LUA;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(lua, loader) {
	duckdb::LoadInternal(loader);
}
}
