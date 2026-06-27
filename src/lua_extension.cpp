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
	args.data[0].ToUnifiedFormat(scriptData);
	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i))) {
			result.SetValue(i, Value(LogicalType::VARCHAR));
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
	result.Verify();
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
	args.data[0].ToUnifiedFormat(scriptData);
	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);

	UnifiedVectorFormat argData;
	args.data[1].ToUnifiedFormat(argData);
	auto argDataPtr = UnifiedVectorFormat::GetData<string_t>(argData);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	std::string resultStr;
	auto jsonError =
	    luaL_loadbuffer(L, DKJSON_SOURCE.c_str(), DKJSON_SOURCE.size(), DKJSON_BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
	if (jsonError) {
		resultStr = ReadLuaResponse(L, jsonError);
	}

	for (idx_t i = 0; i < args.size(); i++) {
		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i))) {
			result.SetValue(i, Value(LogicalType::JSON()));
			continue;
		}
		if (!jsonError) {
			auto script = scriptDataPtr[scriptData.sel->get_index(i)];
			int decodeError = 0;
			if (argData.validity.RowIsValid(argData.sel->get_index(i))) {
				// json.decode
				lua_getfield(L, -1, "decode");
				auto data = argDataPtr[argData.sel->get_index(i)];
				lua_pushlstring(L, data.GetData(), data.GetSize());
				decodeError = lua_pcall(L, 1, 1, 0);
			} else {
				lua_pushnil(L);
			}
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
					lua_pop(L, 1); // remove json.encode
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
	result.Verify();
}

inline void LuaScalarVarcharFun(DataChunk &args, ExpressionState &state, Vector &result) {
	Value contextVarNameValue = Value(LogicalType::VARCHAR);
	state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	UnifiedVectorFormat scriptData;
	args.data[0].ToUnifiedFormat(scriptData);
	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);

	UnifiedVectorFormat argData;
	args.data[1].ToUnifiedFormat(argData);
	auto argDataPtr = UnifiedVectorFormat::GetData<string_t>(argData);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i))) {
			result.SetValue(i, Value(LogicalType::VARCHAR));
			continue;
		}

		auto script = scriptDataPtr[scriptData.sel->get_index(i)];

		if (argData.validity.RowIsValid(argData.sel->get_index(i))) {
			auto data = argDataPtr[argData.sel->get_index(i)];
			lua_pushlstring(L, data.GetData(), data.GetSize());
		} else {
			lua_pushnil(L);
		}
		lua_setglobal(L, contextVarName.c_str());

		// Run the user code
		auto error = luaL_loadbuffer(L, script.GetData(), script.GetSize(), BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
		auto resultStr = ReadLuaResponse(L, error);
		auto resultDuckdbStr = string_t(strdup(resultStr.c_str()), resultStr.size());

		result_data[i] = StringVector::AddString(result, resultDuckdbStr);
	}

	lua_close(L);
	result.Verify();
}

template <typename T, bool IsInteger, bool IsBool>
inline void LuaScalarNumericFun(DataChunk &args, ExpressionState &state, Vector &result) {
	Value contextVarNameValue = Value(LogicalType::VARCHAR);
	state.GetContext().TryGetCurrentSetting(CONTEXT_OPTION_NAME, contextVarNameValue);
	auto contextVarName = contextVarNameValue.GetValue<string>();

	lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	UnifiedVectorFormat scriptData;
	args.data[0].ToUnifiedFormat(scriptData);
	auto scriptDataPtr = UnifiedVectorFormat::GetData<string_t>(scriptData);

	UnifiedVectorFormat argData;
	args.data[1].ToUnifiedFormat(argData);
	auto argDataPtr = UnifiedVectorFormat::GetData<T>(argData);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		if (!scriptData.validity.RowIsValid(scriptData.sel->get_index(i))) {
			result.SetValue(i, Value(LogicalType::VARCHAR));
			continue;
		}

		auto script = scriptDataPtr[scriptData.sel->get_index(i)];

		if (argData.validity.RowIsValid(argData.sel->get_index(i))) {
			auto data = argDataPtr[argData.sel->get_index(i)];
			static_assert(!(IsBool && IsInteger), "LuaScalarNumericFun template invalid");
			if (IsBool) {
				lua_pushboolean(L, data);
			} else if (IsInteger) {
				lua_pushinteger(L, data);
			} else {
				lua_pushnumber(L, data);
			}
		} else {
			lua_pushnil(L);
		}
		lua_setglobal(L, contextVarName.c_str());

		// Run the user code
		auto error = luaL_loadbuffer(L, script.GetData(), script.GetSize(), BUFFER_NAME) || lua_pcall(L, 0, 1, 0);
		auto resultStr = ReadLuaResponse(L, error);
		auto resultDuckdbStr = string_t(strdup(resultStr.c_str()), resultStr.size());

		result_data[i] = StringVector::AddString(result, resultDuckdbStr);
	}

	lua_close(L);
	result.Verify();
}

template <LogicalTypeId ARG, typename T>
static ScalarFunction MakeNumericLuaFunction(FunctionStability stability) {
	auto function =
	    ScalarFunction("lua", {LogicalType::VARCHAR, ARG}, LogicalType::VARCHAR, LuaScalarNumericFun<T, false, false>);
	function.SetStability(stability);
	lua_scalar_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return function;
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription(StringUtil::Format("Lua embedded scripting language, %s", LUA_RELEASE));

	ScalarFunctionSet lua_scalar_functions("lua");

	// We don't know if the user script calls e.g. math.random(), so deoptimize
	auto stability = FunctionStability::VOLATILE;

	auto lua_scalar_function = ScalarFunction("lua", {LogicalType::VARCHAR}, LogicalType::VARCHAR, LuaScalarFun);
	lua_scalar_function.SetStability(stability);
	lua_scalar_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	lua_scalar_functions.AddFunction(lua_scalar_function);

	auto lua_scalar_function_varchar =
	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, LuaScalarVarcharFun);
	lua_scalar_function_varchar.SetStability(stability);
	lua_scalar_function_varchar.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	lua_scalar_functions.AddFunction(lua_scalar_function_varchar);

	auto lua_scalar_function_json =
	    ScalarFunction("lua", {LogicalType::VARCHAR, LogicalType::JSON()}, LogicalType::JSON(), LuaScalarJsonFun);
	lua_scalar_function_json.SetStability(stability);
	lua_scalar_function_json.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	lua_scalar_functions.AddFunction(lua_scalar_function_json);

	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::FLOAT, float>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::DOUBLE, double>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::TINYINT, int8_t>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::UTINYINT, uint8_t>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::SMALLINT, int16_t>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::USMALLINT, uint16_t>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::INTEGER, int32_t>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::UINTEGER, uint32_t>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::BIGINT, int64_t>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::UBIGINT, uint64_t>(stability));
	lua_scalar_functions.AddFunction(MakeNumericLuaFunction<LogicalType::BOOLEAN, bool>(stability));

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
