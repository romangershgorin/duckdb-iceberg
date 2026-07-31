#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "iceberg_functions.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_options.hpp"
#include "iceberg_utils.hpp"
#include "avro_scan.hpp"
#include "manifest_reader.hpp"
#include "metadata/iceberg_table_metadata.hpp"

namespace duckdb {

// ── Helpers ───────────────────────────────────────────────────────────────────

static vector<IcebergManifestListEntry> LoadManifestList(const IcebergSnapshot &snapshot,
                                                         const IcebergTableMetadata &metadata, ClientContext &context,
                                                         const string &iceberg_path, const IcebergOptions &options) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_path = options.allow_moved_paths
	                              ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                              : snapshot.manifest_list;
	auto scan = AvroScan::ScanManifestList(snapshot, metadata, context, manifest_list_path);
	auto reader = make_uniq<manifest_list::ManifestListReader>(*scan);
	vector<IcebergManifestListEntry> result;
	while (!reader->Finished()) {
		reader->Read(STANDARD_VECTOR_SIZE, result);
	}
	return result;
}

static unordered_set<string> CollectDataFilePaths(const IcebergSnapshot &snapshot,
                                                  const vector<IcebergManifestListEntry> &manifest_list,
                                                  const IcebergTableMetadata &metadata, ClientContext &context,
                                                  const string &iceberg_path, const IcebergOptions &options) {
	auto &fs = FileSystem::GetFileSystem(context);
	vector<IcebergManifestListEntry> data_manifests;
	for (auto &m : manifest_list) {
		if (m.file.content == IcebergManifestContentType::DATA) {
			data_manifests.push_back(m);
		}
	}
	if (data_manifests.empty()) {
		return {};
	}
	auto scan = AvroScan::ScanManifest(snapshot, data_manifests, options, fs, iceberg_path, metadata, context);
	auto reader = make_uniq<manifest_file::ManifestReader>(*scan, /*skip_deleted=*/false);
	unordered_set<string> file_paths;
	vector<IcebergManifestEntry> entries;
	while (!reader->Finished()) {
		entries.clear();
		reader->Read(STANDARD_VECTOR_SIZE, entries);
		for (auto &e : entries) {
			file_paths.insert(e.data_file.file_path);
		}
	}
	return file_paths;
}

// ── Bind data ─────────────────────────────────────────────────────────────────

struct IcebergChangesBindData : public TableFunctionData {
	IcebergOptions options;
	vector<pair<string, string>> files; // (file_path, change_type)

	// S3 credentials extracted from the caller's SecretManager so the scan's
	// internal Connection can inherit them (vended creds are connection-scoped).
	string s3_key_id;
	string s3_secret;
	string s3_session_token;
	string s3_region;
	string s3_endpoint;
	string s3_scope;
	bool has_s3_creds = false;
};

// ── Global state ──────────────────────────────────────────────────────────────

struct IcebergChangesGlobalState : public GlobalTableFunctionState {
	idx_t file_idx = 0;
	string current_change_type;
	unique_ptr<Connection> conn;
	unique_ptr<QueryResult> current_result;
	bool done = false;

	explicit IcebergChangesGlobalState(ClientContext &context, const IcebergChangesBindData &bind) {
		done = bind.files.empty();
		if (done) {
			return;
		}
		auto &db = DatabaseInstance::GetDatabase(context);
		conn = make_uniq<Connection>(db);
		// Inject the caller's S3 credentials so parquet reads succeed under
		// S3 Access Grants (vended credentials are connection-scoped in DuckDB).
		if (bind.has_s3_creds) {
			string scope_clause = bind.s3_scope.empty() ? "" : ", SCOPE '" + bind.s3_scope + "'";
			string token_clause =
			    bind.s3_session_token.empty() ? "" : ", SESSION_TOKEN '" + bind.s3_session_token + "'";
			string endpoint_clause = bind.s3_endpoint.empty() ? "" : ", ENDPOINT '" + bind.s3_endpoint + "'";
			auto inject = conn->Query("CREATE OR REPLACE TEMPORARY SECRET __iceberg_changes_s3 ("
			                         "TYPE s3, "
			                         "KEY_ID '" +
			                         bind.s3_key_id + "', " + "SECRET '" + bind.s3_secret + "'" + token_clause +
			                         ", REGION '" + bind.s3_region + "'" + endpoint_clause + scope_clause + ")");
			(void)inject; // ignore errors — worst case falls back to default creds
		}
	}

	bool AdvanceToNextFile(const IcebergChangesBindData &bind) {
		current_result.reset();
		while (file_idx < bind.files.size()) {
			const string &file_path = bind.files[file_idx].first;
			current_change_type = bind.files[file_idx].second;
			file_idx++;
			current_result = conn->Query("SELECT * FROM parquet_scan('" + file_path + "')");
			if (current_result->HasError()) {
				current_result.reset();
				continue;
			}
			return true;
		}
		done = true;
		return false;
	}
};

// ── Bind ──────────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> IcebergChangesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<IcebergChangesBindData>();

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "allow_moved_paths") {
			bind_data->options.allow_moved_paths = BooleanValue::Get(kv.second);
		} else if (loption == "metadata_compression_codec") {
			bind_data->options.metadata_compression_codec = StringValue::Get(kv.second);
		}
	}

	auto input_string = input.inputs[0].ToString();
	auto snap_before_id = input.inputs[1].GetValue<int64_t>();
	auto snap_after_id = input.inputs[2].GetValue<int64_t>();

	auto iceberg_path = IcebergUtils::GetStorageLocation(context, input_string);
	auto &fs = FileSystem::GetFileSystem(context);

	// Load table metadata
	auto meta_path = IcebergTableMetadata::GetMetaDataPath(context, iceberg_path, fs, bind_data->options);
	auto table_metadata_raw = IcebergTableMetadata::Parse(meta_path, fs, bind_data->options.metadata_compression_codec);
	auto metadata = IcebergTableMetadata::FromTableMetadata(table_metadata_raw);

	// Resolve snapshots
	auto snap_before_ptr = metadata.GetSnapshotById(snap_before_id);
	auto snap_after_ptr = metadata.GetSnapshotById(snap_after_id);
	if (!snap_before_ptr) {
		throw InvalidInputException("iceberg_changes: snap_before_id %lld not found in table metadata", snap_before_id);
	}
	if (!snap_after_ptr) {
		throw InvalidInputException("iceberg_changes: snap_after_id %lld not found in table metadata", snap_after_id);
	}
	const IcebergSnapshot &snap_before = *snap_before_ptr;
	const IcebergSnapshot &snap_after = *snap_after_ptr;

	// Load manifest lists and diff data file sets
	auto before_manifest_list = LoadManifestList(snap_before, metadata, context, iceberg_path, bind_data->options);
	auto after_manifest_list = LoadManifestList(snap_after, metadata, context, iceberg_path, bind_data->options);

	auto before_files =
	    CollectDataFilePaths(snap_before, before_manifest_list, metadata, context, iceberg_path, bind_data->options);
	auto after_files =
	    CollectDataFilePaths(snap_after, after_manifest_list, metadata, context, iceberg_path, bind_data->options);

	// Build (file_path, change_type) list
	for (auto &f : before_files) {
		if (after_files.find(f) == after_files.end()) {
			bind_data->files.emplace_back(f, "DELETE");
		}
	}
	for (auto &f : after_files) {
		if (before_files.find(f) == before_files.end()) {
			bind_data->files.emplace_back(f, "INSERT");
		}
	}

	// Extract S3 credentials from the caller's secret manager so the scan's
	// internal Connection can be seeded with them (they're connection-scoped).
	if (!bind_data->files.empty()) {
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		auto &sm = SecretManager::Get(context);
		auto match = sm.LookupSecret(transaction, bind_data->files[0].first, "s3");
		if (match.HasMatch()) {
			auto &kv = dynamic_cast<const KeyValueSecret &>(match.GetSecret());
			bind_data->s3_key_id = kv.TryGetValue("key_id").IsNull() ? "" : kv.TryGetValue("key_id").ToString();
			bind_data->s3_secret = kv.TryGetValue("secret").IsNull() ? "" : kv.TryGetValue("secret").ToString();
			bind_data->s3_session_token =
			    kv.TryGetValue("session_token").IsNull() ? "" : kv.TryGetValue("session_token").ToString();
			bind_data->s3_region = kv.TryGetValue("region").IsNull() ? "" : kv.TryGetValue("region").ToString();
			bind_data->s3_endpoint = kv.TryGetValue("endpoint").IsNull() ? "" : kv.TryGetValue("endpoint").ToString();
			auto &scope = match.GetSecret().GetScope();
			bind_data->s3_scope = scope.empty() ? "" : scope[0];
			bind_data->has_s3_creds = !bind_data->s3_key_id.empty();
		}
	}

	// Schema from Iceberg table metadata — no parquet read needed.
	names.push_back("_change_type");
	return_types.push_back(LogicalType::VARCHAR);
	auto &schema = metadata.GetLatestSchema();
	vector<string> col_names;
	vector<LogicalType> col_types;
	schema.GetColumnNamesAndTypes(col_names, col_types);
	for (idx_t i = 0; i < col_names.size(); i++) {
		names.push_back(col_names[i]);
		return_types.push_back(col_types[i]);
	}

	return std::move(bind_data);
}

// ── Init ──────────────────────────────────────────────────────────────────────

static unique_ptr<GlobalTableFunctionState> IcebergChangesInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<IcebergChangesBindData>();
	return make_uniq<IcebergChangesGlobalState>(context, bind);
}

// ── Scan ──────────────────────────────────────────────────────────────────────

static void IcebergChangesScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<IcebergChangesGlobalState>();
	auto &bind = data_p.bind_data->Cast<IcebergChangesBindData>();

	while (true) {
		if (state.done) {
			output.SetCardinality(0);
			return;
		}

		if (!state.current_result) {
			if (!state.AdvanceToNextFile(bind)) {
				output.SetCardinality(0);
				return;
			}
		}

		auto chunk = state.current_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			state.current_result.reset();
			continue; // try next file
		}

		idx_t n = chunk->size();
		output.SetCardinality(n);

		// Column 0: _change_type — constant per file
		auto change_type_data = FlatVector::GetData<string_t>(output.data[0]);
		for (idx_t i = 0; i < n; i++) {
			change_type_data[i] = StringVector::AddString(output.data[0], state.current_change_type);
		}

		// Columns 1..N: data columns from parquet
		idx_t data_cols = MinValue<idx_t>(chunk->ColumnCount(), output.ColumnCount() - 1);
		for (idx_t i = 0; i < data_cols; i++) {
			VectorOperations::Copy(chunk->data[i], output.data[i + 1], n, 0, 0);
		}
		return;
	}
}

// ── Registration ─────────────────────────────────────────────────────────────

TableFunctionSet IcebergFunctions::GetIcebergChangesFunction() {
	TableFunctionSet function_set("iceberg_changes");
	TableFunction table_function({LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT}, IcebergChangesScan,
	                             IcebergChangesBind, IcebergChangesInitGlobal);
	table_function.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	table_function.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
