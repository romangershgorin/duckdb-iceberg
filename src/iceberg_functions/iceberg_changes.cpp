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
	IcebergOptions options;

	// Pre-read rows: all parquet data fetched during Bind using the caller's
	// ClientContext so Lakekeeper-vended S3 credentials are available.
	struct FileRows {
		string change_type;
		vector<unique_ptr<DataChunk>> chunks;
	};
	vector<FileRows> file_rows;
};

// ── Global state ──────────────────────────────────────────────────────────────

struct IcebergChangesGlobalState : public GlobalTableFunctionState {
	idx_t file_idx = 0;
	idx_t chunk_idx = 0;
	bool done = false;

	explicit IcebergChangesGlobalState(bool empty) : done(empty) {}
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
	vector<pair<string, string>> changed_files;
	for (auto &f : before_files) {
		if (after_files.find(f) == after_files.end()) {
			changed_files.emplace_back(f, "DELETE");
		}
	}
	for (auto &f : after_files) {
		if (before_files.find(f) == before_files.end()) {
			changed_files.emplace_back(f, "INSERT");
		}
	}

	// Read all parquet data now using the caller's ClientContext so that
	// Lakekeeper-vended S3 credentials (connection-scoped) are inherited.
	// Schema is captured from the first successful read.
	vector<LogicalType> data_types;
	vector<string> data_names;

	for (auto &[file_path, change_type] : changed_files) {
		IcebergChangesBindData::FileRows file_rows_entry;
		file_rows_entry.change_type = change_type;

		auto result = context.Query("SELECT * FROM parquet_scan('" + file_path + "')", false);
		if (!result->HasError()) {
			if (data_types.empty()) {
				data_types = result->types;
				data_names = result->names;
			}
			while (true) {
				auto chunk = result->Fetch();
				if (!chunk || chunk->size() == 0) {
					break;
				}
				file_rows_entry.chunks.push_back(std::move(chunk));
			}
		}
		bind_data->file_rows.push_back(std::move(file_rows_entry));
	}

	names.push_back("_change_type");
	return_types.push_back(LogicalType::VARCHAR);
	for (idx_t i = 0; i < data_types.size(); i++) {
		names.push_back(data_names[i]);
		return_types.push_back(data_types[i]);
	}

	return std::move(bind_data);
}

// ── Init ──────────────────────────────────────────────────────────────────────

static unique_ptr<GlobalTableFunctionState> IcebergChangesInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<IcebergChangesBindData>();
	return make_uniq<IcebergChangesGlobalState>(bind.file_rows.empty());
}

// ── Scan ──────────────────────────────────────────────────────────────────────

static void IcebergChangesScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<IcebergChangesGlobalState>();
	auto &bind = data_p.bind_data->Cast<IcebergChangesBindData>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	// Advance past exhausted files
	while (state.file_idx < bind.file_rows.size()) {
		if (state.chunk_idx < bind.file_rows[state.file_idx].chunks.size()) {
			break;
		}
		state.file_idx++;
		state.chunk_idx = 0;
	}

	if (state.file_idx >= bind.file_rows.size()) {
		state.done = true;
		output.SetCardinality(0);
		return;
	}

	auto &file = bind.file_rows[state.file_idx];
	auto &src = *file.chunks[state.chunk_idx++];
	idx_t n = src.size();
	output.SetCardinality(n);

	// Column 0: _change_type — constant per file
	auto change_type_data = FlatVector::GetData<string_t>(output.data[0]);
	for (idx_t i = 0; i < n; i++) {
		change_type_data[i] = StringVector::AddString(output.data[0], file.change_type);
	}

	// Columns 1..N: copy parquet data columns
	idx_t data_cols = MinValue<idx_t>(src.ColumnCount(), output.ColumnCount() - 1);
	for (idx_t i = 0; i < data_cols; i++) {
		VectorOperations::Copy(src.data[i], output.data[i + 1], n, 0, 0);
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
