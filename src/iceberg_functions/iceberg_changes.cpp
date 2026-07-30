#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
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
	vector<pair<string, string>> files; // (file_path, "INSERT" | "DELETE")
	string iceberg_path;
	IcebergOptions options;
};

// ── Global state ──────────────────────────────────────────────────────────────

struct IcebergChangesGlobalState : public GlobalTableFunctionState {
	const vector<pair<string, string>> &files;
	idx_t file_idx = 0;
	string current_change_type;
	unique_ptr<Connection> conn;
	unique_ptr<QueryResult> current_result;
	unique_ptr<DataChunk> held_chunk; // keeps fetched parquet chunk alive across Scan calls
	bool done = false;

	explicit IcebergChangesGlobalState(ClientContext &context, const IcebergChangesBindData &bind) : files(bind.files) {
		auto &db = DatabaseInstance::GetDatabase(context);
		conn = make_uniq<Connection>(db);
	}

	bool AdvanceToNextFile() {
		current_result.reset();
		while (file_idx < files.size()) {
			auto &[file_path, change_type] = files[file_idx++];
			current_change_type = change_type;
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

	bind_data->iceberg_path = IcebergUtils::GetStorageLocation(context, input_string);
	auto &iceberg_path = bind_data->iceberg_path;
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

	// DELETED = in snap_before but not in snap_after (CoW: old files silently dropped)
	for (auto &f : before_files) {
		if (after_files.find(f) == after_files.end()) {
			bind_data->files.emplace_back(f, "DELETE");
		}
	}
	// INSERTED = in snap_after but not in snap_before
	for (auto &f : after_files) {
		if (before_files.find(f) == before_files.end()) {
			bind_data->files.emplace_back(f, "INSERT");
		}
	}

	// Detect output schema from first available parquet file
	if (!bind_data->files.empty()) {
		auto &db = DatabaseInstance::GetDatabase(context);
		auto schema_conn = make_uniq<Connection>(db);
		auto schema_result =
		    schema_conn->Query("SELECT * FROM parquet_scan('" + bind_data->files[0].first + "') LIMIT 0");
		if (!schema_result->HasError()) {
			names.push_back("_change_type");
			return_types.push_back(LogicalType::VARCHAR);
			for (idx_t i = 0; i < schema_result->ColumnCount(); i++) {
				names.push_back(schema_result->names[i]);
				return_types.push_back(schema_result->types[i]);
			}
			return std::move(bind_data);
		}
	}

	// Fallback: only _change_type (no diff or unreadable files)
	names.push_back("_change_type");
	return_types.push_back(LogicalType::VARCHAR);
	return std::move(bind_data);
}

// ── Init ──────────────────────────────────────────────────────────────────────

static unique_ptr<GlobalTableFunctionState> IcebergChangesInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<IcebergChangesBindData>();
	auto state = make_uniq<IcebergChangesGlobalState>(context, bind);
	if (!bind.files.empty()) {
		state->AdvanceToNextFile();
	} else {
		state->done = true;
	}
	return std::move(state);
}

// ── Scan ──────────────────────────────────────────────────────────────────────

static void IcebergChangesScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<IcebergChangesGlobalState>();

	// Release the previous chunk — output from last call was already processed
	state.held_chunk.reset();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	while (true) {
		if (!state.current_result) {
			output.SetCardinality(0);
			return;
		}

		state.held_chunk = state.current_result->Fetch();
		if (!state.held_chunk || state.held_chunk->size() == 0) {
			state.held_chunk.reset();
			if (!state.AdvanceToNextFile()) {
				output.SetCardinality(0);
				return;
			}
			continue;
		}

		idx_t n = state.held_chunk->size();
		output.SetCardinality(n);

		// Column 0: _change_type — constant per file
		auto change_type_data = FlatVector::GetData<string_t>(output.data[0]);
		for (idx_t i = 0; i < n; i++) {
			change_type_data[i] = StringVector::AddString(output.data[0], state.current_change_type);
		}

		// Columns 1..N: parquet data columns (reference held_chunk which outlives this call)
		idx_t data_cols = MinValue<idx_t>(state.held_chunk->ColumnCount(), output.ColumnCount() - 1);
		for (idx_t i = 0; i < data_cols; i++) {
			output.data[i + 1].Reference(state.held_chunk->data[i]);
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
