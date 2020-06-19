#include "sqlite_arrow.h"

#include <chrono>
#include <dirent.h>
#include <fstream>
#include <future>
#include <iostream>

#include <arrow/api.h>
#include <arrow/io/file.h>

typedef std::shared_ptr<arrow::Table> table_ptr;
typedef std::string string;
typedef std::unordered_map<std::string, std::string> string_map;
typedef std::vector<std::string> string_vec;

using arrow::DoubleBuilder;
using arrow::FloatBuilder;
using arrow::Int64Builder;
using arrow::Int32Builder;
using arrow::BinaryBuilder;
using arrow::Int32Array;
using arrow::Int64Array;
using arrow::FloatArray;
using arrow::DoubleArray;
using arrow::BinaryArray;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

enum data_sink_type {Arrow, CppType, Parquet, ArrowTablePerThread};

const int NODES_COUNT = 6;

// split a big vector into n smaller vectors
template<typename T>
std::vector<std::vector<T>> split_vector(const std::vector<T>& vec, size_t n)
{
    std::vector<std::vector<T>> outVec;

    size_t length = vec.size() / n;
    size_t remain = vec.size() % n;

    size_t begin = 0;
    size_t end = 0;

    for (size_t i = 0; i < std::min(n, vec.size()); ++i)
    {
        end += (remain > 0) ? (length + !!(remain--)) : length;
        outVec.push_back(std::vector<T>(vec.begin() + begin, vec.begin() + end));
        begin = end;
    }

    return outVec;
}

// break down a string using delimiter delim.
void tokenize(string const &str, const char delim, string_vec &out)
{
    size_t start;
    size_t end = 0;

    while ((start = str.find_first_not_of(delim, end)) != string::npos)
    {
        end = str.find(delim, start);
        out.push_back(str.substr(start, end - start));
    }
}

string_vec get_all_files_path_per_node(string input_path, string dhl_name, string file_extension, int node_index) {
    // CONSTANTS declaration, could move else where for more flexibility
    string DHL_ROOT_PATH = input_path;

    if (!opendir(DHL_ROOT_PATH.c_str())) {
        DHL_ROOT_PATH = "/Users/wen/github/arrow/data/test_dirs/";
    }

    string DIE_ROW = "dierow_";
    string SWATH = "swath_";
    string CH0PATCH = "channel0." + file_extension;
    string CH1PATCH = "channel1." + file_extension;

    string_vec file_paths;

    string worker_node_path = "R" + std::to_string(node_index) + "C0S/";

    string dhl_path = DHL_ROOT_PATH + worker_node_path + dhl_name;

    // TODO strings for different machine, the strings is one node ONLY
    // for vi3-0009
    //dhl_path = "/Volumes/remoteStorage/" + dhl_name;
    // for windows Linux
    //dhl_path = "/home/wen/github/arrow/data/test_dirs/ROCOS/" + dhl_name;

    std::cout << "Top Level Path = " << dhl_path << std::endl;

    DIR *dhl_dir = nullptr;
    if ((dhl_dir = opendir(dhl_path.c_str())) != NULL) {
        struct dirent *dhl_dir_item;

        while ((dhl_dir_item = readdir(dhl_dir)) != NULL) {
            if (dhl_dir_item->d_type == DT_DIR) {
                string die_row_folder_name = dhl_dir_item->d_name;

                if (die_row_folder_name.find(DIE_ROW) != string::npos) {
                    DIR *die_row_dir = nullptr;
                    string abs_die_row_path = dhl_path + "/" + die_row_folder_name;
                    if ((die_row_dir = opendir(abs_die_row_path.c_str())) != NULL) {
                        struct dirent *die_row_dir_item;
                        while ((die_row_dir_item = readdir(die_row_dir)) != NULL) {
                            if (die_row_dir_item->d_type == DT_DIR) {
                                string swath_folder_name = die_row_dir_item->d_name;

                                if (swath_folder_name.find(SWATH) != string::npos) {
                                    DIR *swath_dir = nullptr;
                                    string abs_swath_path = abs_die_row_path + "/" + swath_folder_name;
                                    if ((swath_dir = opendir(abs_swath_path.c_str())) != NULL) {
                                        struct dirent *swath_dir_item;
                                        while ((swath_dir_item = readdir(swath_dir)) != NULL) {
                                            if (swath_dir_item->d_type == DT_REG) {
                                                string full_path_file_name = swath_dir_item->d_name;
                                                const char delim = '/';
                                                string_vec fileTokens;
                                                tokenize(full_path_file_name, delim, fileTokens);
                                                string file_name_only = fileTokens.back();

                                                if (file_name_only == CH0PATCH || file_name_only == CH1PATCH) {
                                                    //std::cout << "accepted file = " << full_path_file_name << std::endl;
                                                    file_paths.push_back(abs_swath_path + "/" + full_path_file_name);
                                                }
                                            }
                                        }
                                        closedir(swath_dir);
                                    }
                                }
                            }
                        }
                        closedir(die_row_dir);
                    }
                }
            }
        }
        closedir(dhl_dir);
    }
    return file_paths;
}

int get_all_files_path(string input_path, string dhl_name, string file_extension, std::vector<string_vec> &file_paths_all_nodes) {

    std::vector<std::future<string_vec>> futures;
    for (int i = 0; i < NODES_COUNT; i++) {
        std::future<string_vec> future = std::async(std::launch::async, get_all_files_path_per_node, input_path, dhl_name, file_extension, i);
        futures.push_back(std::move(future));
    }

    for (auto&& future : futures) {
        string_vec file_paths = future.get();
        file_paths_all_nodes.push_back(file_paths);
    }

    return EXIT_SUCCESS;
}

/* ################################################### */
// Generate one arrow table per thread, instead of one arrow table per sqlite table
// A thread could handle thousands of sqlite tables
int load_data_to_arrow_v3_one_table_per_thread(
        string file_path,
        std::unordered_map<string, std::shared_ptr<Int64Builder>> &int64_builder_map,
        std::unordered_map<string, std::shared_ptr<DoubleBuilder>> &double_builder_map,
        std::unordered_map<string, std::shared_ptr<FloatBuilder>> &float_builder_map,
        std::unordered_map<string, std::shared_ptr<BinaryBuilder>> &binary_builder_map,
        std::unordered_map<string, std::shared_ptr<Int32Builder>> &int32_builder_map,
        uint64_t &binary_count, uint64_t &binary_size_total, uint64_t &binary_zero_count,
        bool is_random) {

    sqlite3* pDb;
    int flags = (SQLITE_OPEN_READONLY);

    int bResult = sqlite3_open_v2(file_path.c_str(), &pDb, flags, NULL);

    if (SQLITE_OK != bResult) {
        sqlite3_close(pDb);
        std::cerr << "Cannot Open DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            string strMsg = sqlite3_errmsg(pDb);
            sqlite3_close_v2(pDb);
            pDb = nullptr;
        } else {
            std::cerr << "Unable to get DB handle" << std::endl;
        }
        return EXIT_FAILURE;
    }

    //string strKey = "sAr5w3Vk5l";
    string strKey = "e9FkChw3xF";
    bResult = sqlite3_key_v2(pDb, nullptr, strKey.c_str(), static_cast<int>(strKey.size()));

    if (SQLITE_OK != bResult) {
        sqlite3_close(pDb);
        std::cerr << "Cannot key the DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            string strMsg = sqlite3_errmsg(pDb);
            sqlite3_close_v2(pDb);
            pDb = nullptr;
            std::cerr << "SQLite Error Message: " << strMsg << std::endl;
        } else {
            std::cerr << "Unable to key the database" << std::endl;
        }

        return EXIT_FAILURE;
    }

    SqliteUtil* sqliteUtil = new SqliteUtil();
    string query = sqliteUtil->get_query_columns(QUERY_COLUMNS_FILE_NAME);

    if (is_random) {
        query += " WHERE defectKey$defectID IN (";

        srand(time(0));

        int initial = rand() % 10;

        string ids = "";  // construct "1,2,3,4,"
        for (int i = 0; i < 1200; i++) {
            ids += std::to_string(initial + i * 10) + ",";
        }
        ids.pop_back(); // "1,2,3,4"

        query += ids + ");";
    } else {
        query += ";";
    }

    sqlite3_stmt *stmt;
    bResult = sqlite3_prepare_v2(pDb, query.c_str(), -1, &stmt, NULL);

    if (SQLITE_OK != bResult) {
        sqlite3_finalize(stmt);
        sqlite3_close(pDb);
        std::cerr << "Cannot prepare statement from DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            string strMsg = sqlite3_errmsg(pDb);
            sqlite3_close_v2(pDb);
            pDb = nullptr;
            std::cerr << "SQLite Error Message: " << strMsg << std::endl;
        } else {
            std::cerr << "Unable to prepare SQLite statement" << std::endl;
        }

        return EXIT_FAILURE;
    }

    int col_count = sqlite3_column_count(stmt);

    int row_count = 0;
    while ((bResult = sqlite3_step(stmt)) == SQLITE_ROW) {
        row_count++;
        for (int i = 0; i < col_count; i++) {
            string col_name = sqlite3_column_name(stmt, i);
            string col_type = sqlite3_column_decltype(stmt, i);
            //std::cout << i << " col_name = " << col_name << ", col_type = " << col_type << std::endl;

            if ("BIGINT" == col_type) {
                std::shared_ptr<arrow::Int64Builder> builder = int64_builder_map[col_name];
                ABORT_ON_FAILURE(builder->Append(sqlite3_column_int64(stmt, i)));

            } else if ("DOUBLE" == col_type) {
                std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
                double val = sqlite3_column_double(stmt, i);
                ABORT_ON_FAILURE(builder->Append(val));

            } else if ("FLOAT" == col_type) {
                std::shared_ptr<arrow::FloatBuilder> builder = float_builder_map[col_name];
                float val = sqlite3_column_double(stmt, i);
                ABORT_ON_FAILURE(builder->Append(val));

            } else if ("BLOB" == col_type) {
                int blob_size = sqlite3_column_bytes(stmt, i);
                uint8_t *local_buffer;

                if (blob_size > 0) {
                    binary_count++;
                    binary_size_total += blob_size;
                    const uint8_t* blob_ptr = reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt, i));
                    local_buffer = new uint8_t[blob_size];
                    std::copy(blob_ptr, blob_ptr + blob_size, local_buffer);
                } else {
                    // We must populate
                    binary_zero_count++;
                    blob_size = 1;
                    local_buffer = new uint8_t[blob_size];
                    local_buffer[0] = 64;
                }

                std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
                ABORT_ON_FAILURE(builder->Append(local_buffer, blob_size));

                delete local_buffer;
            } else if ("INTEGER" == col_type) {
                std::shared_ptr<arrow::Int32Builder> builder = int32_builder_map[col_name];
                ABORT_ON_FAILURE(builder->Append(sqlite3_column_int(stmt, i)));
            }
        }
    }

    sqlite3_finalize(stmt);
    sqlite3_close(pDb);

    return row_count;
}

/* ################################################### */


table_ptr process_each_data_batch(
        string_vec const &file_paths,
        string_map const &source_schema_map,
        data_sink_type memory_target,
        int thread_id,
        bool has_encrypt,
        int reserve_size,
        bool is_random) {

    // output variable that holds total rows for this data batch
    int sum_num_rows_per_thread = 0;

    if (memory_target == ArrowTablePerThread) {

        arrow::MemoryPool* pool = arrow::default_memory_pool();

        std::unordered_map<string, std::shared_ptr<Int64Builder>> int64_builder_map;
        std::unordered_map<string, std::shared_ptr<DoubleBuilder>> double_builder_map;
        std::unordered_map<string, std::shared_ptr<FloatBuilder>> float_builder_map;
        std::unordered_map<string, std::shared_ptr<BinaryBuilder>> binary_builder_map;
        std::unordered_map<string, std::shared_ptr<Int32Builder>> int32_builder_map;

        for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
            string col_name = itr->first;
            string col_type = itr->second;

            if ("DOUBLE" == col_type) {
                std::shared_ptr<DoubleBuilder> builder = std::make_shared<DoubleBuilder>(arrow::float64(), pool);
                if (reserve_size > 0) {
                    ABORT_ON_FAILURE(builder->Reserve(reserve_size));
                }
                double_builder_map[col_name] = builder;

            } else if ("FLOAT" == col_type) {
                std::shared_ptr<FloatBuilder> builder = std::make_shared<FloatBuilder>(arrow::float32(), pool);
                if (reserve_size > 0) {
                    ABORT_ON_FAILURE(builder->Reserve(reserve_size));
                }
                float_builder_map[col_name] = builder;

            } else if ("BIGINT" == col_type) {
                std::shared_ptr<Int64Builder> builder = std::make_shared<Int64Builder>(arrow::int64(), pool);
                if (reserve_size > 0) {
                    ABORT_ON_FAILURE(builder->Reserve(reserve_size));
                }
                int64_builder_map[col_name] = builder;

            } else if ("INTEGER" == col_type) {
                std::shared_ptr<Int32Builder> builder = std::make_shared<Int32Builder>(arrow::int32(), pool);
                if (reserve_size > 0) {
                    ABORT_ON_FAILURE(builder->Reserve(reserve_size));
                }
                int32_builder_map[col_name] = builder;

            } else if ("BLOB" == col_type) {
                std::shared_ptr<BinaryBuilder> builder = std::make_shared<BinaryBuilder>(arrow::binary(), pool);
                if (reserve_size > 0) {
                    ABORT_ON_FAILURE(builder->Reserve(reserve_size));
                }
                binary_builder_map[col_name] = builder;
            }
        }

        int table_count = 0;
        uint64_t binary_count = 0;
        uint64_t binary_zero_count = 0;
        uint64_t binary_size_total = 0;
        for (auto file_path : file_paths) {
            sum_num_rows_per_thread +=
                    load_data_to_arrow_v3_one_table_per_thread(
                            file_path,
                            int64_builder_map,
                            double_builder_map,
                            float_builder_map,
                            binary_builder_map,
                            int32_builder_map,
                            binary_count,
                            binary_size_total,
                            binary_zero_count,
                            is_random);

            table_count++;
        }

//        std::cout << "Finished builder appending, memory alloc:" << pool->bytes_allocated() << ", max: " << pool->max_memory()
//                  << ", total rows:" << sum_num_rows_per_thread << ", table#: " << table_count
//                  << ", binary count: " << binary_count << ", zero_count: " << binary_zero_count << ", size: " << binary_size_total
//                  << std::endl;

        std::cout << "Now we start merging " << table_count << " tables...." << std::endl;

        // Two tasks are accomplished in here:
        // 1. create arrow array for each column.  The whole arrays object will be used to create an Arrow Table
        // 2. create a vector of Field (schema) along the way
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
            string col_name = itr->first;
            string col_type = itr->second;

            std::shared_ptr<arrow::Array> array;
            if ("BIGINT" == col_type) {
                fields.emplace_back(arrow::field(col_name, arrow::int64()));

                std::shared_ptr<arrow::Int64Builder> builder = int64_builder_map[col_name];
                ABORT_ON_FAILURE(builder->Finish(&array));

            } else if ("DOUBLE" == col_type) {
                fields.emplace_back(arrow::field(col_name, arrow::float64()));

                std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
                ABORT_ON_FAILURE(builder->Finish(&array));

            } else if ("FLOAT" == col_type) {
                fields.emplace_back(arrow::field(col_name, arrow::float32()));

                std::shared_ptr<arrow::FloatBuilder> builder = float_builder_map[col_name];
                ABORT_ON_FAILURE(builder->Finish(&array));

            } else if ("BLOB" == col_type) {
                fields.emplace_back(arrow::field(col_name, arrow::binary()));

                std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
                ABORT_ON_FAILURE(builder->Finish(&array));

            } else if ("INTEGER" == col_type) {
                fields.emplace_back(arrow::field(col_name, arrow::int32()));

                std::shared_ptr<arrow::Int32Builder> builder = int32_builder_map[col_name];
                ABORT_ON_FAILURE(builder->Finish(&array));
            }

            arrays.emplace_back(array);
        }

        auto schema = std::make_shared<arrow::Schema>(fields);
        table_ptr result_table = arrow::Table::Make(schema, arrays);

        std::cout << "After merging " << table_count << " tables, row size = " << result_table->num_rows() << ", thread id = " << thread_id << std::endl;

        // this table contains all the data per thread
        return result_table;
    }

    return nullptr;
}

int SqliteArrow::sqlite_to_arrow(string dhl_name, string input_path, bool is_random, table_ptr* table) {
    // default parameters
    string file_extension = "patch";
    int thread_count_per_node = 1;
    data_sink_type sink_target = ArrowTablePerThread;
    bool has_encrypt = false;
    int reserve_size = 0; // memory reservation size for Arrow Array

    std::cout << "DHL: " << dhl_name << ", root path: " << input_path << ", extension: " << file_extension
              << ", thread count per node: " << thread_count_per_node << std::endl;

    SqliteUtil* sqliteUtil = new SqliteUtil();
    std::cout << "The first 200 characters of query string: " << sqliteUtil->get_query_columns(QUERY_COLUMNS_FILE_NAME).substr(0, 200) << std::endl;

    auto start = std::chrono::steady_clock::now();

    // we will create one vector of patch files per node
    std::vector<string_vec> file_paths_all_nodes;
    get_all_files_path(input_path, dhl_name, file_extension, file_paths_all_nodes);

    auto stop1 = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = stop1 - start;
    std::cout << "Patch file paths collection finished. The elapsed time: " << elapsed_seconds.count() << " seconds\n";

    std::cout << "File paths vector total count (controls node-level threading) = " << file_paths_all_nodes.size() << std::endl;
    for (auto file_paths: file_paths_all_nodes) {
        std::cout << "Files count per node = " << file_paths.size() << std::endl;
    }

    // create data source db schema, as it's needed for arrow table creation
    // it's better to get schema here, because every thread need the same schema object
    string_map source_schema_map;

    SqliteUtil* sqliteUtil1 = new SqliteUtil();
    sqliteUtil1->get_schema(file_paths_all_nodes.front().front(), source_schema_map);
    //print_schema(source_schema_map);

    std::vector<std::future<table_ptr>> futures;

    // file_pathes_all_nodes has NODES_COUNT file_paths.  Each file_paths has all the files on that node
    int thread_id = 1;

    for (auto file_paths : file_paths_all_nodes) {
        auto vec_with_thread_count = split_vector(file_paths, thread_count_per_node);

        std::cout << "This node will have thread count = " << vec_with_thread_count.size() << std::endl;

        for (auto files : vec_with_thread_count) {
            std::future<table_ptr> future = std::async(
                    std::launch::async,
                    process_each_data_batch,
                    files,
                    source_schema_map,
                    sink_target,
                    thread_id++,
                    has_encrypt,
                    reserve_size,
                    is_random);

            futures.push_back(std::move(future));
        }
    }

    std::cout << "All threads have been started...." << std::endl;

    std::vector<table_ptr> tables;

    for (auto&& future : futures) {
        table_ptr table = future.get();
        tables.push_back(table);
    }

    auto stop2 = std::chrono::steady_clock::now();
    elapsed_seconds = stop2 - stop1;
    std::cout << "All threads finishing reading data, it takes: " << elapsed_seconds.count() << " seconds\n";

    arrow::Result<table_ptr> result = arrow::ConcatenateTables(tables);
    table_ptr result_table = result.ValueOrDie();
    std::cout << "Final merging " << tables.size() << " tables into one arrow table, total row size = " << result_table->num_rows() << std::endl;

    *table = result_table;

    auto end = std::chrono::steady_clock::now();
    elapsed_seconds = end - stop2;
    std::cout << "Merging into 1 arrow table takes: " << elapsed_seconds.count() << " seconds\n";

    elapsed_seconds = end - start;
    std::cout << "Total elapsed time from start to finish: " << elapsed_seconds.count() << "s\n";

    return 0;
}

int SqliteArrow::arrow_to_sqlite(table_ptr table, string output_file_path) {
    std::shared_ptr<arrow::Schema> schema = table->schema();
    std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();

    std::cout << "Starting to create SQLite table at: " << output_file_path << std::endl;

    std::vector<int64_t> array_size_vec;
    if (table->column(0)->num_chunks() > 1) {
        std::cout << "More than one chunk found: " << table->column(0)->num_chunks() << std::endl;
        std::cout << "Number of rows: " << table->num_rows() << std::endl;

        std::shared_ptr<arrow::ChunkedArray> chunkedArray = table->column(0);
        for (std::shared_ptr<arrow::Array> array :chunkedArray->chunks()) {
            std::cout << "Array Vector length: " << array->length() << std::endl;
            array_size_vec.push_back(array->length());
        }
    }

    // holds the comma separated string of column names. e.g. "col1, col2, col3"
    string col_string = "";

    // holds comma separated string of (col_name sqlite_col_type) pair.  This is for SQLite table construction
    string schema_string = "";

    // comma separated question mark string to satisfy sqlite's prepared statement. e.g. ?,?,?,?
    string question_marks = "";

    // upsert requires update pairs, e.g. col1 = ?, col2 = ?, col3 = ?
    string upsert_pairs = "";
    for (auto&& field : fields) {
        string col_name = field->name();
        arrow::Type::type col_type = field->type()->id();

        if (arrow::Type::INT32 == col_type) {
            //int32_array_map[col_name] = std::static_pointer_cast<Int32Array>(table->GetColumnByName(col_name)->chunk(0));
            schema_string += col_name + " INTEGER,";

        } else if (arrow::Type::INT64 == col_type) {
            //int64_array_map[col_name] = std::static_pointer_cast<Int64Array>(table->GetColumnByName(col_name)->chunk(0));
            schema_string += col_name + " BIGINT,";

        } else if (arrow::Type::FLOAT == col_type) {
            //float_array_map[col_name] = std::static_pointer_cast<FloatArray>(table->GetColumnByName(col_name)->chunk(0));
            schema_string += col_name + " FLOAT,";

        } else if (arrow::Type::DOUBLE == col_type) {
            //double_array_map[col_name] = std::static_pointer_cast<DoubleArray>(table->GetColumnByName(col_name)->chunk(0));
            schema_string += col_name + " DOUBLE,";

        } else if (arrow::Type::BINARY == col_type) {
            //binary_array_map[col_name] = std::static_pointer_cast<BinaryArray>(table->GetColumnByName(col_name)->chunk(0));
            schema_string += col_name + " BLOB,";

        } else {
            // There is a new data type from the table creator.  What should we do next?
            continue;
        }
        col_string += col_name;
        col_string += ",";
        question_marks += "?,";
        upsert_pairs += col_name + "=?,";
    }

    // remove the trailing comma
    col_string.pop_back();
    schema_string.pop_back();
    question_marks.pop_back();
    upsert_pairs.pop_back();

    //std::cout << "col_string = " << col_string << std::endl;
    //std::cout << "questions marks = " << question_marks << std::endl;

    sqlite3* pDb;
    int flags = (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);

    int bResult = sqlite3_open_v2(output_file_path.c_str(), &pDb, flags, NULL);

    if (SQLITE_OK != bResult) {
        sqlite3_close(pDb);
        std::cerr << "Cannot Open v2 DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            string strMsg = sqlite3_errmsg(pDb);
            std::cerr << "Error Message: " << strMsg << std::endl;
            sqlite3_close_v2(pDb);
            pDb = nullptr;
        } else {
            std::cerr << "Unable to get DB handle" << std::endl;
        }
        return EXIT_FAILURE;
    }

//    bResult = sqlite3_key_v2(pDb, nullptr, DHL_KEY.c_str(), static_cast<int>(DHL_KEY.size()));
//
//    if (SQLITE_OK != bResult) {
//        sqlite3_close(pDb);
//        std::cerr << "Cannot key the DB: " << bResult << std::endl;
//
//        if (nullptr != pDb) {
//            string strMsg = sqlite3_errmsg(pDb);
//            sqlite3_close_v2(pDb);
//            pDb = nullptr;
//            std::cerr << "SQLite Error Message: " << strMsg << std::endl;
//        } else {
//            std::cerr << "Unable to key the database" << std::endl;
//        }
//
//        return EXIT_FAILURE;
//    }

    sqlite3_mutex_enter(sqlite3_db_mutex(pDb));
    char* errorMessage;
    sqlite3_exec(pDb, "PRAGMA synchronous=OFF", NULL, NULL, &errorMessage);
    sqlite3_exec(pDb, "PRAGMA count_changes=OFF", NULL, NULL, &errorMessage);
    sqlite3_exec(pDb, "PRAGMA journal_mode=MEMORY", NULL, NULL, &errorMessage);
    sqlite3_exec(pDb, "PRAGMA temp_store=MEMORY", NULL, NULL, &errorMessage);

    sqlite3_exec(pDb, "BEGIN TRANSACTION", NULL, NULL, &errorMessage);

    string create_sql = "CREATE TABLE IF NOT EXISTS attribTable(" + schema_string + ");";

    bResult = sqlite3_exec(pDb, create_sql.c_str(), NULL, 0, &errorMessage);

    if (SQLITE_OK != bResult) {
        sqlite3_close(pDb);
        std::cerr << "Cannot execute Create table command: " << bResult << std::endl;

        if (nullptr != pDb) {
            string strMsg = sqlite3_errmsg(pDb);
            std::cerr << "Error Message: " << strMsg << std::endl;
            sqlite3_close_v2(pDb);
            pDb = nullptr;
        } else {
            std::cerr << "Unable to get DB handle" << std::endl;
        }
    }

    sqlite3_stmt *stmt;

    string insert_sql = "INSERT INTO attribTable (" + col_string + ") VALUES (" + question_marks + ");";

    string upsert = "ON CONFLICT(defectKey$defectID) DO UPDATE SET ";
    upsert += upsert_pairs;
    upsert += ";";

    //insert_sql += upsert;

    //std::cout << "upsert = " + upsert << std::endl;
//    string upsert = "INSERT INTO attribTable (defectId, polarity, class)";
//    upsert += "  VALUES('1234','1','abc')"
//    upsert += "  ON CONFLICT(defectId) DO UPDATE SET"
//    upsert += "    polarity=excluded.phonenumber,"
//    upsert += "    class=excluded.validDate\n"
//    upsert += "  WHERE excluded.validDate > phonebook2.validDate;";

    bResult = sqlite3_prepare(pDb, insert_sql.c_str(), -1, &stmt, NULL);

    if (bResult != SQLITE_OK) {
        sqlite3_finalize(stmt);
        std::cerr << "Cannot prepare statement from DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            string strMsg = sqlite3_errmsg(pDb);
            sqlite3_close_v2(pDb);
            pDb = nullptr;
            std::cerr << "SQLite Error Message: " << strMsg << std::endl;
        } else {
            std::cerr << "Unable to prepare SQLite statement" << std::endl;
        }

        return EXIT_FAILURE;
    }

    int chunk_idx = 0;
    int64_t array_idx = 0;
    for (int64_t row_idx = 0; row_idx < table->num_rows(); row_idx++) {
        int col_idx = 0;
        for (auto&& field : fields) {
            string col_name = field->name();
            arrow::Type::type col_type = field->type()->id();

//            if (!array_size_vec.empty()) {
//                // Since there are multiple chunks, we must calculate the true chunk_idx and array_idx
//
//                // make sure we always increment chunk_idx when our current array_idx
//                // is bigger than current array size
//                if (array_idx >= array_size_vec(chunk_idx)) {
//                    chunk_idx += 1;
//                    array_idx = 0;
//                    // at this moment, we start over with new array.
//                }
//
//                std::vector<int64_t>::iterator it = array_size_vec.begin();
//                array_idx cumul_sum = std::accumulate(it, it + chunk_idx - 1, 0);
//
//                array_idx = row_idx - cumul_sum;
//            }

            array_idx = row_idx;

            if (arrow::Type::INT32 == col_type) {
                auto array = std::static_pointer_cast<Int32Array>(table->GetColumnByName(col_name)->chunk(chunk_idx));
                if (array->length() > array_idx) {
                    sqlite3_bind_int(stmt, col_idx, array->Value(array_idx));
                }

            } else if (arrow::Type::INT64 == col_type) {
                auto array = std::static_pointer_cast<Int64Array>(table->GetColumnByName(col_name)->chunk(chunk_idx));
                if (array->length() > array_idx) {
                    sqlite3_bind_int64(stmt, col_idx, array->Value(array_idx));
                }

            } else if (arrow::Type::FLOAT == col_type) {
                auto array = std::static_pointer_cast<FloatArray>(table->GetColumnByName(col_name)->chunk(chunk_idx));
                if (array->length() > array_idx) {
                    sqlite3_bind_double(stmt, col_idx, array->Value(array_idx));
                }

            } else if (arrow::Type::DOUBLE == col_type) {
                auto array = std::static_pointer_cast<DoubleArray>(table->GetColumnByName(col_name)->chunk(chunk_idx));
                if (array->length() > array_idx) {
                    sqlite3_bind_double(stmt, col_idx, array->Value(array_idx));
                }

            } else if (arrow::Type::BINARY == col_type) {
                auto array = std::static_pointer_cast<BinaryArray>(table->GetColumnByName(col_name)->chunk(chunk_idx));
                if (array->length() > array_idx) {
                    int length;
                    const uint8_t* local_buffer = NULL;
                    local_buffer = array->GetValue(array_idx, &length);
//                sqlite3_bind_blob(stmt, col_idx, local_buffer, length, SQLITE_STATIC);

                    uint8_t *local_buffer2 = new uint8_t[length];
                    local_buffer2[0] = 66;
                    sqlite3_bind_blob(stmt, col_idx, local_buffer2, length, SQLITE_STATIC);
                    delete[] local_buffer2;

                }

            } else {
                // There is a new data type from the table creator.  What should we do next?
                continue;
            }

            col_idx++;
        }

        int retVal = sqlite3_step(stmt);
        if (retVal != SQLITE_DONE)
        {
            printf("Commit Failed! %d\n", retVal);
        }

        sqlite3_reset(stmt);
    }

    sqlite3_exec(pDb, "COMMIT TRANSACTION", NULL, NULL, &errorMessage);
    sqlite3_finalize(stmt);

    sqlite3_mutex_leave(sqlite3_db_mutex(pDb));
    sqlite3_close(pDb);

    return EXIT_SUCCESS;
}

// Automatically split the arrow table into smaller table and export to distinct sqlite files concurrently
// if output_paths specified, num_partitions will be overwritten by the output_paths size
int SqliteArrow::arrow_to_sqlite_split(table_ptr table, int num_partitions, std::vector<string> output_paths) {
    if (output_paths.size() > 0) {
        num_partitions = output_paths.size();
    }

    if (num_partitions > table->num_rows()) {
        std::cout << "Please make sure your table size is equal or bigger than number of partitions." << std::endl;
        return EXIT_FAILURE;
    }

    int total_rows = table->num_rows();
    int remainder = total_rows % num_partitions;
    int num_rows_per_table = (total_rows + remainder) / num_partitions;

    std::vector<table_ptr> split_tables;

    for (int i = 0; i < num_partitions; i++) {

        table_ptr slice = table->Slice(i * num_rows_per_table, num_rows_per_table);

        split_tables.push_back(slice);
    }

    std::cout << "Table with " << total_rows << " rows are split into " << num_partitions << " partitions." << std::endl;
    std::cout << "This table column 0 has chunk size: " << table->column(0)->num_chunks() << std::endl;
    std::cout << "This table column 1 has chunk size: " << table->column(1)->num_chunks() << std::endl;

    for (table_ptr split_table : split_tables) {
        std::cout << "Slice has num rows: " << split_table->num_rows() << ", num_col: " << split_table->num_columns() << std::endl;
        std::cout << "Slice column 0 has chunk size: " << table->column(0)->num_chunks() << std::endl;
        std::cout << "Slice column 1 has chunk size: " << table->column(1)->num_chunks() << std::endl;
    }

    std::vector<std::future<int>> futures;

    for (int i = 0; i < num_partitions; i++) {
        std::future<int> future =
                std::async(
                        std::launch::async,
                        &SqliteArrow::arrow_to_sqlite,
                        this,
                        split_tables[i],
                        output_paths[i]);
        futures.push_back(std::move(future));
    }

    std::cout << futures.size() << " saving-to-sqlite threads have been started...." << std::endl;

    for (auto&& future : futures) {
        future.get();
    }

    std::cout << "All threads finished their work.  The total number of files is " << num_partitions << std::endl;

    return 0;
}

