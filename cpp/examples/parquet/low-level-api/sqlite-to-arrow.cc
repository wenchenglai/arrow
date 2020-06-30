/*
 * 2020/06/248
 * Read multiple DHL files randomly, and update values to the same files that were loaded from
 * This simulates the scenario where we need to upsert into a DHLx.
 *
 * The assumption is another process will provide a list of IDs
 *
 * This test simply load the data and write a few columns back using the IDs provided.
 *
 * We make one SQLite table as an Arrow RecordBatch, so the key challenge is to split
 * a giant Arrow Table into thousands of RecordBatch(es).  The next key challenge is sort
 * an Arrow Table with multiple columns
 *
 * usage:
 * local test:
 * ./sqlite-to-arrow test_dhl /Users/wen/github/arrow/data/test_dirs
 *
 * */

#include <cassert>
#include <chrono>
#include <dirent.h>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <thread>

#include <arrow/api.h>
#include <arrow/io/file.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include "arrow/util/decimal.h"
#include "arrow/testing/util.h"
#include "arrow/status.h"

#include "sqlite3.h"

typedef std::shared_ptr<arrow::Table> table_ptr;
typedef std::string string;
typedef std::unordered_map<std::string, std::string> string_map;
typedef std::vector<std::string> string_vec;

using arrow::DoubleBuilder;
using arrow::FloatBuilder;
using arrow::Int64Builder;
using arrow::Int32Builder;
using arrow::BinaryBuilder;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

string get_query_columns(string);

enum data_sink_type {Arrow, CppType, Parquet, ArrowTablePerThread};

const string QUERY_COLUMNS_FILE_NAME = "columns.txt";
const string PARQUET = ".parquet";
const int NODES_COUNT = 6;
const string col_encryp_key_id = "key1";
const string col_encryp_key = "9874567896123459";
const string footer_encryp_key_id = "key2";
const string footer_encryp_key = "9123856789712348";
//constexpr int64_t ROW_GROUP_SIZE = 512;  //

string CANONICAL_QUERY_STRING = get_query_columns(QUERY_COLUMNS_FILE_NAME);

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

string_vec get_all_files_path_per_node(string dhl_name, string file_extension, int node_index) {
    // CONSTANTS declaration, could move else where for more flexibility
    string DHL_ROOT_PATH = "/mnt/nodes/";

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

int get_all_files_path(string dhl_name, string file_extension, std::vector<string_vec> &file_paths_all_nodes) {

    std::vector<std::future<string_vec>> futures;
    for (int i = 0; i < NODES_COUNT; i++) {
        std::future<string_vec> future = std::async(std::launch::async, get_all_files_path_per_node, dhl_name, file_extension, i);
        futures.push_back(std::move(future));
    }

    for (auto&& future : futures) {
        string_vec file_paths = future.get();
        file_paths_all_nodes.push_back(file_paths);
    }

    return EXIT_SUCCESS;
}

// print SQLite Data Source Schema
void print_dhl_sqlite_schema(string_map const &source_schema_map) {
    std::cout << "******** Schema ******** = " << std::endl;
    int i = 1;
    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        std::cout << i++ << ": " << itr->first << "  " << itr->second << std::endl;
    }
}

// Read the columns that we need to use from a file on disk
string get_query_columns(string file_name) {
    std::ifstream in_file;

    in_file.open(file_name);

    if (!in_file) {
        return "Select * FROM attribTable";
    }

    int size = 65535;
    char columns[size];
    in_file.getline(columns, size);

    in_file.close();

    return "SELECT " + string(columns) + " FROM attribTable";
}

// This function will get the SQLite data source schema.  We need to load this dynamically to create destination type,
// which is typically an Arrow table.
int get_schema(string file_path, string_map& source_schema_map) {
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

        return 0;
    }

    string query = CANONICAL_QUERY_STRING + " LIMIT 1;";

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

        return 0;
    }

    int col_count = sqlite3_column_count(stmt);
    std::cout << "Total column count is " << col_count << std::endl;

    // create source db schema map as "column name: data type"
    // we need this to generalize data scanning to create arrow column builder
    // Also, arrow table creation needs to build similar schema
    for (int i = 0; i < col_count; i++) {
        string col_name = sqlite3_column_name(stmt, i);
        string col_type = sqlite3_column_decltype(stmt, i);
        source_schema_map[col_name] = col_type;
    }
    return EXIT_SUCCESS;
}

int load_data_to_arrow(
        string file_path,
        string_map const &source_schema_map,
        table_ptr* table,
        int reserve_size) {

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
                PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
            }
            double_builder_map[col_name] = builder;

        } else if ("FLOAT" == col_type) {
            std::shared_ptr<FloatBuilder> builder = std::make_shared<FloatBuilder>(arrow::float32(), pool);
            if (reserve_size > 0) {
                PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
            }
            float_builder_map[col_name] = builder;

        } else if ("BIGINT" == col_type) {
            std::shared_ptr<Int64Builder> builder = std::make_shared<Int64Builder>(arrow::int64(), pool);
            if (reserve_size > 0) {
                PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
            }
            int64_builder_map[col_name] = builder;

        } else if ("INTEGER" == col_type) {
            std::shared_ptr<Int32Builder> builder = std::make_shared<Int32Builder>(arrow::int32(), pool);
            if (reserve_size > 0) {
                PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
            }
            int32_builder_map[col_name] = builder;

        } else if ("BLOB" == col_type) {
            std::shared_ptr<BinaryBuilder> builder = std::make_shared<BinaryBuilder>(arrow::binary(), pool);
            if (reserve_size > 0) {
                PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
            }
            binary_builder_map[col_name] = builder;
        }
    }

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

    string query = CANONICAL_QUERY_STRING + ";";

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

            if ("BIGINT" == col_type) {
                std::shared_ptr<arrow::Int64Builder> builder = int64_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Append(sqlite3_column_int64(stmt, i)));

            } else if ("DOUBLE" == col_type) {
                std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
                double val = sqlite3_column_double(stmt, i);
                PARQUET_THROW_NOT_OK(builder->Append(val));

            } else if ("FLOAT" == col_type) {
                std::shared_ptr<arrow::FloatBuilder> builder = float_builder_map[col_name];
                float val = sqlite3_column_double(stmt, i);
                PARQUET_THROW_NOT_OK(builder->Append(val));

            } else if ("BLOB" == col_type) {
                int blob_size = sqlite3_column_bytes(stmt, i);

                const uint8_t *pBuffer;
                if (blob_size > 0) {
                    pBuffer = reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt, i));
                } else {
                    // blob_size is zero, what should we do?
                    //std::cout << col_name << " blob_size is zero" << std::endl;
                    blob_size = 1;
                    pBuffer = new uint8_t[blob_size];
//                    //std::copy(pBuffer, pBuffer + blob_size, &buffer[0]);
//                    std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
//                    PARQUET_THROW_NOT_OK(builder->Append(local_buffer, blob_size));
                }

                std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Append(pBuffer, blob_size));
            } else if ("INTEGER" == col_type) {
                std::shared_ptr<arrow::Int32Builder> builder = int32_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Append(sqlite3_column_int(stmt, i)));
            }
        }
        //break;
    }

//    if (bResult != SQLITE_DONE) {
//        std::cerr << "SELECT failed: " << sqlite3_errmsg(pDb) << ".  The result value is " << bResult << std::endl;
//        // if you return/throw here, don't forget the finalize
//    }

    sqlite3_finalize(stmt);
    sqlite3_close(pDb);

    if (row_count == 0) {
        table = nullptr;
        return 0;
    }

    // Two tasks are accomplished in here:
    // 1. create arrow array for each column.  The whole arrays object will be used to create an Arrow Table
    // 2. create a vector of Field (schema) along the way
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        string col_name = itr->first;
        string col_type = itr->second;

        std::shared_ptr<arrow::Array> array;
        if ("DOUBLE" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::float64()));
            std::shared_ptr<DoubleBuilder> builder = double_builder_map[col_name];
            PARQUET_THROW_NOT_OK(builder->Finish(&array));

        } else if ("FLOAT" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::float32()));
            std::shared_ptr<FloatBuilder> builder = float_builder_map[col_name];
            PARQUET_THROW_NOT_OK(builder->Finish(&array));

        } else if ("BIGINT" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::int64()));
            std::shared_ptr<Int64Builder> builder = int64_builder_map[col_name];
            PARQUET_THROW_NOT_OK(builder->Finish(&array));

        } else if ("INTEGER" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::int32()));
            std::shared_ptr<Int32Builder> builder = int32_builder_map[col_name];
            PARQUET_THROW_NOT_OK(builder->Finish(&array));

        } else if ("BLOB" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::binary()));
            std::shared_ptr<BinaryBuilder> builder = binary_builder_map[col_name];
            PARQUET_THROW_NOT_OK(builder->Finish(&array));
        }

        arrays.emplace_back(array);
    }

    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    *table = arrow::Table::Make(schema, arrays);

    //std::cout << "Arrows Loaded " << table->num_rows() << " total rows in " << table->num_columns() << " columns." << std::endl;

    return row_count;
}

/* ################################################### */
int load_data_to_arrow_v3_one_table_per_thread(
        string file_path,
        std::unordered_map<string, std::shared_ptr<Int64Builder>> &int64_builder_map,
        std::unordered_map<string, std::shared_ptr<DoubleBuilder>> &double_builder_map,
        std::unordered_map<string, std::shared_ptr<FloatBuilder>> &float_builder_map,
        std::unordered_map<string, std::shared_ptr<BinaryBuilder>> &binary_builder_map,
        std::unordered_map<string, std::shared_ptr<Int32Builder>> &int32_builder_map,
        uint64_t &binary_count, uint64_t &binary_size_total, uint64_t &binary_zero_count) {

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

    string query = CANONICAL_QUERY_STRING + ";";

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
                PARQUET_THROW_NOT_OK(builder->Append(sqlite3_column_int64(stmt, i)));

            } else if ("DOUBLE" == col_type) {
                std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
                double val = sqlite3_column_double(stmt, i);
                PARQUET_THROW_NOT_OK(builder->Append(val));

            } else if ("FLOAT" == col_type) {
                std::shared_ptr<arrow::FloatBuilder> builder = float_builder_map[col_name];
                float val = sqlite3_column_double(stmt, i);
                PARQUET_THROW_NOT_OK(builder->Append(val));

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
                    binary_zero_count++;
                    blob_size = 1;
                    local_buffer = new uint8_t[blob_size];
                    local_buffer[0] = 64;
                }

                std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Append(local_buffer, blob_size));

                delete local_buffer;
            } else if ("INTEGER" == col_type) {
                std::shared_ptr<arrow::Int32Builder> builder = int32_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Append(sqlite3_column_int(stmt, i)));
            }
        }
    }

    sqlite3_finalize(stmt);
    sqlite3_close(pDb);

    return row_count;
}

table_ptr process_each_data_batch(
        string_vec const &file_paths,
        string_map const &source_schema_map,
        data_sink_type memory_target,
        int thread_id,
        bool has_encrypt,
        int reserve_size) {

    if (memory_target == ArrowTablePerThread) {

        arrow::MemoryPool* pool = arrow::default_memory_pool();
        std::cout << "Memory Pool Type: " << pool->backend_name() << std::endl;

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
                    PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
                }
                double_builder_map[col_name] = builder;

            } else if ("FLOAT" == col_type) {
                std::shared_ptr<FloatBuilder> builder = std::make_shared<FloatBuilder>(arrow::float32(), pool);
                if (reserve_size > 0) {
                    PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
                }
                float_builder_map[col_name] = builder;

            } else if ("BIGINT" == col_type) {
                std::shared_ptr<Int64Builder> builder = std::make_shared<Int64Builder>(arrow::int64(), pool);
                if (reserve_size > 0) {
                    PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
                }
                int64_builder_map[col_name] = builder;

            } else if ("INTEGER" == col_type) {
                std::shared_ptr<Int32Builder> builder = std::make_shared<Int32Builder>(arrow::int32(), pool);
                if (reserve_size > 0) {
                    PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
                }
                int32_builder_map[col_name] = builder;

            } else if ("BLOB" == col_type) {
                std::shared_ptr<BinaryBuilder> builder = std::make_shared<BinaryBuilder>(arrow::binary(), pool);
                if (reserve_size > 0) {
                    PARQUET_THROW_NOT_OK(builder->Reserve(reserve_size));
                }
                binary_builder_map[col_name] = builder;
            }
        }

        int table_count = 0;
        uint64_t binary_count = 0;
        uint64_t binary_zero_count = 0;
        uint64_t binary_size_total = 0;
        int sum_num_rows_per_thread = 0;

        for (auto file_path : file_paths) {
            sum_num_rows_per_thread += load_data_to_arrow_v3_one_table_per_thread(file_path, int64_builder_map, double_builder_map,
                                                                                  float_builder_map, binary_builder_map, int32_builder_map, binary_count, binary_size_total, binary_zero_count);

            table_count++;

//            std::cout << "Memory alloc:" << pool->bytes_allocated() << ", max: " << pool->max_memory()
//            << ", total rows:" << sum_num_rows_per_thread << ", table#: " << table_count
//            << ", binary count: " << binary_count << ", zero_count: " << binary_zero_count << ", size: " << binary_size_total
//            << std::endl;
        }

        std::cout << "Finished builder appending, memory alloc:" << pool->bytes_allocated() << ", max: " << pool->max_memory()
                  << ", total rows:" << sum_num_rows_per_thread << ", table#: " << table_count
                  << ", binary count: " << binary_count << ", zero_count: " << binary_zero_count << ", size: " << binary_size_total
                  << std::endl;

        std::cout << "Now we start merging " << table_count << " tables...." << std::endl;

        // Two tasks are accomplished in here:
        // 1. create arrow array for each column.  The whole arrays object will be used to create an Arrow Table
        // 2. create a vector of Field (schema) along the way
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        std::vector<std::shared_ptr<arrow::Field>> schema_vector;
        for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
            string col_name = itr->first;
            string col_type = itr->second;

            std::shared_ptr<arrow::Array> array;
            if ("BIGINT" == col_type) {
                schema_vector.emplace_back(arrow::field(col_name, arrow::int64()));

                std::shared_ptr<arrow::Int64Builder> builder = int64_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Finish(&array));

            } else if ("DOUBLE" == col_type) {
                schema_vector.emplace_back(arrow::field(col_name, arrow::float64()));

                std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Finish(&array));

            } else if ("FLOAT" == col_type) {
                schema_vector.emplace_back(arrow::field(col_name, arrow::float32()));

                std::shared_ptr<arrow::FloatBuilder> builder = float_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Finish(&array));

            } else if ("BLOB" == col_type) {
                schema_vector.emplace_back(arrow::field(col_name, arrow::binary()));

                std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Finish(&array));

            } else if ("INTEGER" == col_type) {
                schema_vector.emplace_back(arrow::field(col_name, arrow::int32()));

                std::shared_ptr<arrow::Int32Builder> builder = int32_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Finish(&array));
            }

            arrays.emplace_back(array);
        }

        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        table_ptr result_table = arrow::Table::Make(schema, arrays);

        std::cout << "After merging " << table_count << " tables, row size = " << result_table->num_rows() << ", thread id = " << thread_id << std::endl;

        // this table contains all the data per thread
        return result_table;
    }

    return nullptr;
}

int main(int argc, char** argv) {
    string dhl_name = "";
    string file_extension = "patch";
    int thread_count_per_node = 1;
    data_sink_type sink_target = Arrow;
    bool has_encrypt = true;
    int reserve_size = 0;

    // Print Help message
    if(argc == 2 && strcmp(argv[1], "-h") == 0) {
        std::cout << "Parameters List" << std::endl;
        std::cout << "1: name of DHL" << std::endl;
        std::cout << "2: source file types" << std::endl;
        std::cout << "3: thread counts, multiple of 6" << std::endl;
        std::cout << "4: detination types" << std::endl;
        std::cout << "5: turn on/off parquet encryption" << std::endl;
        std::cout << "6: builder reserve size" << std::endl;
        std::cout << "sqlite-to-parquet test_dhl patch|patchAttr|patchAttr340M 6|12|24|48 arrow|cppType|parquet|arrow2 1|0 13000" << std::endl;
        return 0;
    }

    if (argc > 1) {
        dhl_name = argv[1];
    }

    if (argc > 2) {
        file_extension = argv[2];
    }

    if (argc > 3) {
        int input_thread_count = std::stoi(argv[3]);

        if (input_thread_count < NODES_COUNT) {
            input_thread_count = NODES_COUNT;
        }

        thread_count_per_node = input_thread_count / NODES_COUNT;
    }

    if (argc > 4) {
        string target = argv[4];

        if ("cppType" == target) {
            sink_target = CppType;
        } else if ("parquet" == target) {
            sink_target = Parquet;
        } else if ("arrow2" == target) {
            sink_target = ArrowTablePerThread;
        }
    }

    if (argc > 5) {
        string encrypt = argv[5];

        if ("0" == encrypt) {
            has_encrypt = false;
        }
    }

    if (argc > 6) {
        reserve_size = std::stoi(argv[6]);
    }

    if (dhl_name == "") {
        std::cout << "Please specify a DHL name" << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "DHL: " << dhl_name << ", extension: " << file_extension
              << ", thread count per node: " << thread_count_per_node << ", Sink type: " << sink_target
              << ", reserve size: " << reserve_size << std::endl;

    std::cout << "The first 200 characters of query string: " << CANONICAL_QUERY_STRING.substr(0, 200) << std::endl;

    auto start = std::chrono::steady_clock::now();

    // we will create one vector of patch files per node
    std::vector<string_vec> file_paths_all_nodes;
    get_all_files_path(dhl_name, file_extension, file_paths_all_nodes);

    auto stop1 = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = stop1 - start;
    std::cout << "Patch file paths collection finished. The elapsed time: " << elapsed_seconds.count() << " seconds\n";

    for (auto file_paths: file_paths_all_nodes) {
        std::cout << "Files count per node = " << file_paths.size() << std::endl;
    }

    // create data source db schema, as it's needed for arrow table creation
    // it's better to get schema here, because every thread need the same schema object
    string_map source_schema_map;
    get_schema(file_paths_all_nodes.front().front(), source_schema_map);
    //print_schema(source_schema_map);

    std::vector<std::future<table_ptr>> futures;

    // file_pathes_all_nodes has NODES_COUNT file_paths.  Each file_paths has all the files on that node
    int thread_id = 1;

    for (auto file_paths : file_paths_all_nodes) {
        auto vec_with_thread_count = split_vector(file_paths, thread_count_per_node);

        std::cout << "This node will have thread count = " << vec_with_thread_count.size() << std::endl;

        for (auto files : vec_with_thread_count) {
            //total_row_count += process_each_data_batch(files, source_schema_map, sink_target, thread_id++, has_encrypt, reserve_size);

            std::future<table_ptr> future = std::async(std::launch::async, process_each_data_batch, files,
                                                 source_schema_map, sink_target, thread_id++, has_encrypt, reserve_size);
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

    auto end = std::chrono::steady_clock::now();
    elapsed_seconds = end - stop2;
    std::cout << "Merging into 1 arrow table takes: " << elapsed_seconds.count() << " seconds\n";

    elapsed_seconds = end - start;
    std::cout << "Total elapsed time from start to finish: " << elapsed_seconds.count() << "s\n";

    return 0;
}


