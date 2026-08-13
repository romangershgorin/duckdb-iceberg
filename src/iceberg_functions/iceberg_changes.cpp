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

// Builds a DuckDB array literal from a list of S3 paths: ['s3://a', 's3://b']
static string BuildFileList(const vector<string> &files) {
	string result = "[";
	for (idx_t i = 0; i < files.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += "'" + files[i] + "'";
	}
	result += "]";
	return result;
}

// Builds "left.col1 = right.col1 AND left.col2 = right.col2 ..." for a JOIN/EXISTS clause.
static string BuildJoinCondition(const vector<string> &cols, const string &left, const string &right) {
	string result;
	for (idx_t i = 0; i < cols.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		result += left + "." + cols[i] + " = " + right + "." + cols[i];
	}
	return result;
}

// Builds the net-diff SQL query over the changed files only.
//
// Without identifier_columns (keyless): whole-row IS NOT DISTINCT FROM comparison,
// equivalent to Spark's create_changelog_view with net_changes=true. Carry-over rows
// that appear identically in both deleted and inserted files cancel out.
//
// With identifier_columns: pairs DELETE+INSERT rows that share the same key into
// UPDATE_BEFORE/UPDATE_AFTER. Unmatched DELETEs/INSERTs are emitted as-is.
//
// With identifier_columns + net_changes=true: collapses UPDATE pairs to just INSERT
// (the final state). Only rows deleted with no matching key in inserts are emitted
// as DELETE. Equivalent to Spark's net_changes=true — simplifies CDC consumers.
static string BuildNetDiffQuery(const vector<string> &delete_files, const vector<string> &insert_files,
                                const vector<string> &identifier_columns, bool net_changes) {
	if (delete_files.empty() && insert_files.empty()) {
		return "";
	}
	if (delete_files.empty()) {
		return "SELECT 'INSERT' AS _change_type, * FROM parquet_scan(" + BuildFileList(insert_files) + ")";
	}
	if (insert_files.empty()) {
		return "SELECT 'DELETE' AS _change_type, * FROM parquet_scan(" + BuildFileList(delete_files) + ")";
	}
	string del_list = BuildFileList(delete_files);
	string ins_list = BuildFileList(insert_files);

	if (identifier_columns.empty()) {
		// Keyless whole-row net-diff
		return "WITH deleted AS (SELECT * FROM parquet_scan(" + del_list + ")), "
		       "inserted AS (SELECT * FROM parquet_scan(" + ins_list + ")) "
		       "SELECT 'DELETE' AS _change_type, d.* FROM deleted d "
		       "WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i IS NOT DISTINCT FROM d) "
		       "UNION ALL "
		       "SELECT 'INSERT' AS _change_type, i.* FROM inserted i "
		       "WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d IS NOT DISTINCT FROM i)";
	}

	// Keyed net-diff: pairs UPDATE_BEFORE/UPDATE_AFTER by identifier_columns,
	// or collapses them to INSERT when net_changes=true.
	string jdi = BuildJoinCondition(identifier_columns, "d", "i");
	string jid = BuildJoinCondition(identifier_columns, "i", "d");
	string shared_ctes = "WITH deleted AS (SELECT * FROM parquet_scan(" + del_list + ")), "
	                     "inserted AS (SELECT * FROM parquet_scan(" + ins_list + ")), "
	                     "net_deletes AS ("
	                     "  SELECT d.* FROM deleted d "
	                     "  WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i IS NOT DISTINCT FROM d)), "
	                     "net_inserts AS ("
	                     "  SELECT i.* FROM inserted i "
	                     "  WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d IS NOT DISTINCT FROM i)) ";

	if (net_changes) {
		// Collapse UPDATE pairs: emit only the final state (INSERT), drop old state.
		// Only rows removed with no matching key in inserts are emitted as DELETE.
		return shared_ctes +
		       "SELECT 'INSERT' AS _change_type, i.* FROM net_inserts i "
		       "UNION ALL "
		       "SELECT 'DELETE' AS _change_type, d.* FROM net_deletes d "
		       "  WHERE NOT EXISTS (SELECT 1 FROM net_inserts i WHERE " + jdi + ")";
	}

	return shared_ctes +
	       "SELECT 'UPDATE_BEFORE' AS _change_type, d.* FROM net_deletes d "
	       "  INNER JOIN net_inserts i ON " + jdi + " "
	       "UNION ALL "
	       "SELECT 'UPDATE_AFTER' AS _change_type, i.* FROM net_inserts i "
	       "  INNER JOIN net_deletes d ON " + jid + " "
	       "UNION ALL "
	       "SELECT 'DELETE' AS _change_type, d.* FROM net_deletes d "
	       "  WHERE NOT EXISTS (SELECT 1 FROM net_inserts i WHERE " + jdi + ") "
	       "UNION ALL "
	       "SELECT 'INSERT' AS _change_type, i.* FROM net_inserts i "
	       "  WHERE NOT EXISTS (SELECT 1 FROM net_deletes d WHERE " + jid + ")";
}

// ── Bind data ─────────────────────────────────────────────────────────────────

struct IcebergChangesBindData : public TableFunctionData {
	IcebergOptions options;
	vector<string> delete_files;
	vector<string> insert_files;
	vector<string> identifier_columns;
	bool net_changes = false;

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
	unique_ptr<Connection> conn;
	unique_ptr<QueryResult> current_result;
	bool done = false;

	explicit IcebergChangesGlobalState(ClientContext &context, const IcebergChangesBindData &bind) {
		done = bind.delete_files.empty() && bind.insert_files.empty();
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
		string query = BuildNetDiffQuery(bind.delete_files, bind.insert_files, bind.identifier_columns, bind.net_changes);
		current_result = conn->Query(query);
		if (current_result->HasError()) {
			current_result.reset();
			done = true;
		}
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
		} else if (loption == "identifier_columns") {
			for (auto &child : ListValue::GetChildren(kv.second)) {
				bind_data->identifier_columns.push_back(StringValue::Get(child));
			}
		} else if (loption == "net_changes") {
			bind_data->net_changes = BooleanValue::Get(kv.second);
		}
	}

	auto input_string = input.inputs[0].ToString();
	auto snap_before_id = input.inputs[1].GetValue<int64_t>();
	auto snap_after_id = input.inputs[2].GetValue<int64_t>();

	auto iceberg_path = IcebergUtils::GetStorageLocation(context, input_string);
	auto &fs = FileSystem::GetFileSystem(context);

	// When resolving a catalog reference, GetStorageLocation returns the metadata
	// file path (s3://.../metadata/00240-....json), not the table root. Path
	// construction in LoadManifestList (allow_moved_paths mode) requires the table
	// root. Strip the /metadata/... suffix to recover it.
	string table_root = iceberg_path;
	if (StringUtil::EndsWith(table_root, ".json")) {
		auto lpath = StringUtil::Lower(table_root);
		auto found = lpath.rfind("/metadata/");
		if (found != string::npos) {
			table_root = table_root.substr(0, found);
		}
	}

	// Load table metadata (iceberg_path may already be a metadata JSON path)
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

	// Load manifest lists and diff data file sets using table_root for path construction
	auto before_manifest_list = LoadManifestList(snap_before, metadata, context, table_root, bind_data->options);
	auto after_manifest_list = LoadManifestList(snap_after, metadata, context, table_root, bind_data->options);

	auto before_files =
	    CollectDataFilePaths(snap_before, before_manifest_list, metadata, context, table_root, bind_data->options);
	auto after_files =
	    CollectDataFilePaths(snap_after, after_manifest_list, metadata, context, table_root, bind_data->options);

	// Set-difference: files dropped → delete_files, files added → insert_files
	for (auto &f : before_files) {
		if (after_files.find(f) == after_files.end()) {
			bind_data->delete_files.push_back(f);
		}
	}
	for (auto &f : after_files) {
		if (before_files.find(f) == before_files.end()) {
			bind_data->insert_files.push_back(f);
		}
	}

	// Extract S3 credentials from the caller's secret manager so the scan's
	// internal Connection can be seeded with them (they're connection-scoped).
	string first_file;
	if (!bind_data->delete_files.empty()) {
		first_file = bind_data->delete_files[0];
	} else if (!bind_data->insert_files.empty()) {
		first_file = bind_data->insert_files[0];
	}
	if (!first_file.empty()) {
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		auto &sm = SecretManager::Get(context);
		auto match = sm.LookupSecret(transaction, first_file, "s3");
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

	if (state.done || !state.current_result) {
		output.SetCardinality(0);
		return;
	}

	auto chunk = state.current_result->Fetch();
	if (!chunk || chunk->size() == 0) {
		output.SetCardinality(0);
		state.done = true;
		return;
	}

	idx_t n = chunk->size();
	output.SetCardinality(n);

	// All columns (including _change_type at index 0) come from the net-diff query result.
	idx_t col_count = MinValue<idx_t>(chunk->ColumnCount(), output.ColumnCount());
	for (idx_t i = 0; i < col_count; i++) {
		VectorOperations::Copy(chunk->data[i], output.data[i], n, 0, 0);
	}
}

// ── Registration ─────────────────────────────────────────────────────────────

TableFunctionSet IcebergFunctions::GetIcebergChangesFunction() {
	TableFunctionSet function_set("iceberg_changes");
	TableFunction table_function({LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT}, IcebergChangesScan,
	                             IcebergChangesBind, IcebergChangesInitGlobal);
	table_function.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	table_function.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	table_function.named_parameters["identifier_columns"] = LogicalType::LIST(LogicalType::VARCHAR);
	table_function.named_parameters["net_changes"] = LogicalType::BOOLEAN;
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
