#define DUCKDB_EXTENSION_MAIN

#include "blob_list_casting_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

template <typename T>
static bool CastBlobToList(Vector &source, Vector &result, idx_t count,
                           CastParameters &parameters) {
  UnaryExecutor::Execute<string_t, list_entry_t>(
      source, result, count, [&](string_t blob) -> list_entry_t {
        auto current_size = ListVector::GetListSize(result);
        if (blob.GetSize() % sizeof(T) != 0) {
          throw InvalidInputException(
              "Blob size is not a multiple of the target type size");
        }
        auto output_length = blob.GetSize() / sizeof(T);

        auto new_size = current_size + output_length;
        if (ListVector::GetListCapacity(result) < new_size) {
          ListVector::Reserve(result, new_size);
        }

        auto &child_entry = ListVector::GetEntry(result);
        auto *child_vals = FlatVector::GetData<T>(child_entry);
        auto &child_validity = FlatVector::Validity(child_entry);

        // Set to all valid
        const char *blob_data = blob.GetData();
        for (idx_t i = 0; i < output_length; i++) {
          child_validity.Set(i + current_size, true);
          child_vals[i + current_size] = *((T *)(&blob_data[i * sizeof(T)]));
        }

        ListVector::SetListSize(result, new_size);

        return {current_size, output_length};
      });

  result.Verify(count);
  return true;
}

template <typename T>
static bool CastListToBlob(Vector &source, Vector &result, idx_t count,
                           CastParameters &parameters) {
  UnaryExecutor::Execute<list_entry_t, string_t>(
      source, result, count, [&](list_entry_t list_entry) -> string_t {
        auto &child_vector = ListVector::GetEntry(source);
        const auto *child_data = FlatVector::GetData<T>(child_vector);
        // Check that all entries are valid
        for (idx_t i = 0; i < list_entry.length; i++) {
          if (!FlatVector::Validity(child_vector)
                   .RowIsValid(list_entry.offset + i)) {
            throw InvalidInputException("List contains invalid entries");
          }
        }

        if (list_entry.length == 0) {
          return StringVector::AddStringOrBlob(result, nullptr, 0);
        }

        return StringVector::AddStringOrBlob(
            result,
            reinterpret_cast<const char *>(&child_data[list_entry.offset]),
            list_entry.length * sizeof(T));
      });

  result.Verify(count);
  return true;
}

template <typename T>
static BoundCastInfo BindCastListToBlob(BindCastInput &input,
                                        const LogicalType &source,
                                        const LogicalType &target) {
  return {CastListToBlob<T>};
}

template <typename T>
static BoundCastInfo BindCastBlobToList(BindCastInput &input,
                                        const LogicalType &source,
                                        const LogicalType &target) {
  return {CastBlobToList<T>};
}


static void LoadInternal(DatabaseInstance &instance) {
    // Register the cast functions
  auto &config = DBConfig::GetConfig(instance);
  auto &casts = config.GetCastFunctions();

  auto cast_cost =
      casts.ImplicitCastCost(LogicalType::SQLNULL, LogicalTypeId::STRUCT) + 1;

  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::TINYINT),
                             LogicalType::BLOB, BindCastListToBlob<int8_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::SMALLINT),
                             LogicalType::BLOB, BindCastListToBlob<int16_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::INTEGER),
                             LogicalType::BLOB, BindCastListToBlob<int32_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::BIGINT),
                             LogicalType::BLOB, BindCastListToBlob<int64_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::HUGEINT),
                             LogicalType::BLOB, BindCastListToBlob<hugeint_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::UTINYINT),
                             LogicalType::BLOB, BindCastListToBlob<uint8_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::USMALLINT),
                             LogicalType::BLOB, BindCastListToBlob<uint16_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::UINTEGER),
                             LogicalType::BLOB, BindCastListToBlob<uint32_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::UBIGINT),
                             LogicalType::BLOB, BindCastListToBlob<uint64_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::UHUGEINT),
                             LogicalType::BLOB, BindCastListToBlob<uhugeint_t>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::FLOAT),
                             LogicalType::BLOB, BindCastListToBlob<float>,
                             cast_cost);
  casts.RegisterCastFunction(LogicalType::LIST(LogicalType::DOUBLE),
                             LogicalType::BLOB, BindCastListToBlob<double>,
                             cast_cost);

  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::TINYINT),
                             BindCastBlobToList<int8_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::SMALLINT),
                             BindCastBlobToList<int16_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::INTEGER),
                             BindCastBlobToList<int32_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::BIGINT),
                             BindCastBlobToList<int64_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::HUGEINT),
                             BindCastBlobToList<hugeint_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::UTINYINT),
                             BindCastBlobToList<uint8_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::USMALLINT),
                             BindCastBlobToList<uint16_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::UINTEGER),
                             BindCastBlobToList<uint32_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::UBIGINT),
                             BindCastBlobToList<uint64_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::UHUGEINT),
                             BindCastBlobToList<uhugeint_t>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::FLOAT),
                             BindCastBlobToList<float>, cast_cost);
  casts.RegisterCastFunction(LogicalType::BLOB,
                             LogicalType::LIST(LogicalType::DOUBLE),
                             BindCastBlobToList<double>, cast_cost);
}

void BlobListCastingExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string BlobListCastingExtension::Name() {
	return "blob_list_casting";
}

std::string BlobListCastingExtension::Version() const {
#ifdef EXT_VERSION_BLOB_LIST_CASTING
	return EXT_VERSION_BLOB_LIST_CASTING;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void blob_list_casting_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::BlobListCastingExtension>();
}

DUCKDB_EXTENSION_API const char *blob_list_casting_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
