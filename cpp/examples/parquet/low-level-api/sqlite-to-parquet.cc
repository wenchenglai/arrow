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

#include "sqlite3.h"

typedef std::shared_ptr<arrow::Table> table_ptr;
typedef std::string string;
typedef std::unordered_map<std::string, std::string> string_map;
typedef std::vector<std::string> string_vec;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

string get_query_columns(string);

enum data_sink_type {Arrow, CppType, Parquet};

const string QUERY_COLUMNS_FILE_NAME = "columns.txt";
const string PARQUET = ".parquet";
const int NODES_COUNT = 6;
const int PARQ_ROW_GROUP_SIZE = 1000;
const string col_encryp_key_id = "key1";
const string col_encryp_key = "9874567896123459";
const string footer_encryp_key_id = "key2";
const string footer_encryp_key = "9123856789712348";
constexpr int64_t ROW_GROUP_SIZE = 128 * 1024 * 1024;  // 128 MB
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

// print row group data
void print_data(int64_t rows_read, int16_t definition_level, int16_t repetition_level, int64_t value, int64_t values_read, int i) {
    std::cout << "rows_read = " << rows_read << std::endl;
    std::cout << "values_read = " << values_read << std::endl;
    std::cout << "value = " << value << std::endl;
    std::cout << "repetition_level = " << repetition_level << std::endl;
    std::cout << "definition_level = " << definition_level << std::endl;
    std::cout << "i = " << i << std::endl;
}

// print parquet file level schema
void print_metadata(std::shared_ptr<parquet::FileMetaData> file_metadata) {
    // Get the number of RowGroups
    int num_row_groups = file_metadata->num_row_groups();
    //assert(num_row_groups == 1);
    std::cout << "Number of Row Groups = " << num_row_groups << std::endl;

    // Get the number of Columns
    int num_columns = file_metadata->num_columns();
    std::cout << "Number of Columns = " << num_columns << std::endl;
    //assert(num_columns == 8);

    // Get the number of Rows
    int num_rows = file_metadata->num_rows();
    std::cout << "Number of Rows = " << num_rows << std::endl;

    // Get the number of Rows
    auto creator = file_metadata->created_by();
    std::cout << "Created by = " << creator << std::endl;

    auto num_elements = file_metadata->num_schema_elements();
    std::cout << "Number of Schema Elements = " << num_elements << std::endl;

    auto is_encrypted = file_metadata->is_encryption_algorithm_set();
    std::cout << "Has Encryption? = " << is_encrypted << std::endl;
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
    dhl_path = "/Volumes/remoteStorage/" + dhl_name;
    // for windows Linux
    //dhl_path = "/home/wen/github/arrow/data/test_dirs/" + dhl_name;

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

        // TODO
        break;
    }

    for (auto&& future : futures) {
        string_vec file_paths = future.get();
        file_paths_all_nodes.push_back(file_paths);
    }

    return EXIT_SUCCESS;
}

// print SQLite Data Source Schema
void print_schema(string_map const &source_schema_map) {
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

// Write out the data as a Parquet file.  It'll also encrypt the output file.
void write_parquet_file(const arrow::Table& table, int node_id, string_map const &source_schema_map, bool has_encrypt) {
    parquet::WriterProperties::Builder wp_builder;

    if (has_encrypt) {
        std::map<string, std::shared_ptr<parquet::ColumnEncryptionProperties>> encryption_cols;

        // we always encrypt all columns
        for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
            string column_name = itr->first;
            parquet::ColumnEncryptionProperties::Builder encryption_col_builder(column_name);
            encryption_col_builder.key(col_encryp_key)->key_id(col_encryp_key_id);
            encryption_cols[column_name] = encryption_col_builder.build();
        }

        parquet::FileEncryptionProperties::Builder file_encryption_builder(footer_encryp_key);
        wp_builder.encryption(file_encryption_builder.footer_key_metadata(footer_encryp_key_id)->encrypted_columns(encryption_cols)->build());
    }

    wp_builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> props = wp_builder.build();

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile,arrow::io::FileOutputStream::Open(std::to_string(node_id) + PARQUET));

    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, PARQ_ROW_GROUP_SIZE, props));

//    std::cout << "Num of cols:" << table.num_columns() << std::endl;
//
//    std::shared_ptr<arrow::Schema> schema = table.schema();
//
//    std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();
//
//    std::cout << "Num of fields: " << fields.size() << std::endl;
//
//    for (auto field : fields) {
//        std::shared_ptr<arrow::ChunkedArray> ca = table.GetColumnByName(field->name());
//        std::cout << field->name() << ": " << ca->length() << std::endl;
//    }
}


int load_data_to_cpp_type(string file_path) {
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
        for (int i = 0; i < col_count; i++) {
            string col_name = sqlite3_column_name(stmt, i);
            string col_type = sqlite3_column_decltype(stmt, i);
            //std::cout << i << " col_name = " << col_name << ", col_type = " << col_type << std::endl;

            if ("BIGINT" == col_type) {
                int64_t val = sqlite3_column_int64(stmt, i);
                val -= val;

            } else if (("DOUBLE" == col_type || "FLOAT" == col_type)) {
                double val = sqlite3_column_double(stmt, i);
                val -= val;

            } else if ("BLOB" == col_type) {
                int blob_size = sqlite3_column_bytes(stmt, i);
                if (blob_size > 0) {
                    const uint8_t *pBuffer = reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt, i));
                    uint8_t buffer[blob_size];
                    std::copy(pBuffer, pBuffer + blob_size, &buffer[0]);
                }

            } else if ("INTEGER" == col_type) {
                int val = sqlite3_column_int(stmt, i);
                val -= val;
            }
        }
        row_count++;
    }

    if (bResult != SQLITE_DONE) {
        std::cerr << "SELECT failed: " << sqlite3_errmsg(pDb) << ".  The result value is " << bResult << std::endl;
        // if you return/throw here, don't forget the finalize
    }

    sqlite3_finalize(stmt);
    sqlite3_close(pDb);

    return row_count;
}

int load_data_to_arrow(
        string file_path,
        string_map const &source_schema_map,
        table_ptr* table) {

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    //arrow::MemoryPool* pool = arrow::system_memory_pool();
    //arrow::MemoryPool* pool = arrow::default_memory_pool();
    //arrow::MemoryPool* pool;
    //ABORT_ON_FAILURE(arrow::jemalloc_memory_pool(&pool));
    //ARROW_RETURN_NOT_OK(arrow::mimalloc_memory_pool(&pool));


    std::unordered_map<string, std::shared_ptr<arrow::Int64Builder>> int64_builder_map;
    std::unordered_map<string, std::shared_ptr<arrow::DoubleBuilder>> double_builder_map;
    std::unordered_map<string, std::shared_ptr<arrow::FloatBuilder>> float_builder_map;
    std::unordered_map<string, std::shared_ptr<arrow::BinaryBuilder>> binary_builder_map;
    std::unordered_map<string, std::shared_ptr<arrow::Int32Builder>> int_builder_map;

    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        if ("BIGINT" == itr->second) {
            int64_builder_map[itr->first] = std::make_shared<arrow::Int64Builder>(arrow::int64(), pool);

        } else if ("DOUBLE" == itr->second) {
            double_builder_map[itr->first] = std::make_shared<arrow::DoubleBuilder>(pool);

        } else if ("FLOAT" == itr->second) {
            float_builder_map[itr->first] = std::make_shared<arrow::FloatBuilder>(pool);

        } else if ("BLOB" == itr->second) {
            binary_builder_map[itr->first] = std::make_shared<arrow::BinaryBuilder>(pool);

        } else if ("INTEGER" == itr->second) {
            int_builder_map[itr->first] = std::make_shared<arrow::Int32Builder>(pool);

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

    while ((bResult = sqlite3_step(stmt)) == SQLITE_ROW) {
        for (int i = 0; i < col_count; i++) {
            string col_name = sqlite3_column_name(stmt, i);
            string col_type = sqlite3_column_decltype(stmt, i);
            //std::cout << i << " col_name = " << col_name << ", col_type = " << col_type << std::endl;

            if ("BIGINT" == col_type && int64_builder_map.find(col_name) != int64_builder_map.end()) {
                std::shared_ptr<arrow::Int64Builder> builder = int64_builder_map[col_name];
                PARQUET_THROW_NOT_OK(builder->Append(sqlite3_column_int64(stmt, i)));

            } else if (("DOUBLE" == col_type) && double_builder_map.find(col_name) != double_builder_map.end()) {
                std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
                double val = sqlite3_column_double(stmt, i);
                PARQUET_THROW_NOT_OK(builder->Append(val));

            } else if (("FLOAT" == col_type) && double_builder_map.find(col_name) != double_builder_map.end()) {
                std::shared_ptr<arrow::FloatBuilder> builder = float_builder_map[col_name];
                double val = sqlite3_column_double(stmt, i);
                PARQUET_THROW_NOT_OK(builder->Append(val));

            } else if ("BLOB" == col_type && binary_builder_map.find(col_name) != binary_builder_map.end()) {
                int blob_size = sqlite3_column_bytes(stmt, i);
                if (blob_size > 0) {
                    const uint8_t *pBuffer = reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt, i));
                    //uint8_t buffer[blob_size];
                    //std::copy(pBuffer, pBuffer + blob_size, &buffer[0]);

                    std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
                    PARQUET_THROW_NOT_OK(builder->Append(pBuffer, blob_size));
                } else {
                    // blob_size is zero, what should we do?
                    //std::cout << col_name << " blob_size is zero" << std::endl;
                    blob_size = 1;
                    uint8_t local_buffer[blob_size];
                    //std::copy(pBuffer, pBuffer + blob_size, &buffer[0]);
                    std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
                    PARQUET_THROW_NOT_OK(builder->Append(local_buffer, blob_size));
                }
            } else if ("INTEGER" == col_type && int_builder_map.find(col_name) != int_builder_map.end()) {
                std::shared_ptr<arrow::Int32Builder> builder = int_builder_map[col_name];
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

    // Two tasks are accomplished in here:
    // 1. create arrow array for each column.  The whole arrays object will be used to create an Arrow Table
    // 2. create a vector of Field (schema) along the way
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        string col_name = itr->first;
        string col_type = itr->second;

        std::shared_ptr<arrow::Array> array;
        if ("BIGINT" == col_type && int64_builder_map.find(col_name) != int64_builder_map.end()) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::int64()));

            std::shared_ptr<arrow::Int64Builder> builder = int64_builder_map[col_name];
            PARQUET_THROW_NOT_OK(builder->Finish(&array));

        } else if (("DOUBLE" == col_type || "FLOAT" == col_type)
                   && double_builder_map.find(col_name) != double_builder_map.end()) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::float64()));

            std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
            PARQUET_THROW_NOT_OK(builder->Finish(&array));

        } else if ("BLOB" == col_type && binary_builder_map.find(col_name) != binary_builder_map.end()) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::binary()));

            std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
            PARQUET_THROW_NOT_OK(builder->Finish(&array));

        } else if ("INTEGER" == col_type && int_builder_map.find(col_name) != int_builder_map.end()) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::int32()));

            std::shared_ptr<arrow::Int32Builder> builder = int_builder_map[col_name];
            PARQUET_THROW_NOT_OK(builder->Finish(&array));
        }

        arrays.emplace_back(array);
    }

    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    *table = arrow::Table::Make(schema, arrays);

    //std::cout << "Arrows Loaded " << table->num_rows() << " total rows in " << table->num_columns() << " columns." << std::endl;

    return 1;
}

int load_data_to_parquet(string file_path,
        string_map const &source_schema_map,
        parquet::RowGroupWriter* rg_writer,
        std::vector<int64_t> &buffered_values_estimate) {

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

    std::cout << "query = " << query << std::endl;

    int num_columns = sqlite3_column_count(stmt);
    std::cout << "number of columns = " << num_columns << std::endl;

    while ((bResult = sqlite3_step(stmt)) == SQLITE_ROW) {
        for (int col_id = 0; col_id < num_columns; col_id++) {
            string col_name = sqlite3_column_name(stmt, col_id);
            string col_type = sqlite3_column_decltype(stmt, col_id);

            std::cout << col_id << " ";
            if ("BIGINT" == col_type) {
                int64_t val = sqlite3_column_int64(stmt, col_id);
                std::cout << "column name = " << col_name << ", type = " << col_type << ", val = " << val << std::endl;
                parquet::Int64Writer* int64_writer = static_cast<parquet::Int64Writer*>(rg_writer->column(col_id));
                int64_writer->WriteBatch(1, nullptr, nullptr, &val);
                buffered_values_estimate[col_id] = int64_writer->EstimatedBufferedValueBytes();

            } else if ("FLOAT" == col_type) {
                float val = sqlite3_column_double(stmt, col_id);
                std::cout << "column name = " << col_name << ", type = " << col_type << ", val = " << val << std::endl;
                parquet::FloatWriter* float_writer = static_cast<parquet::FloatWriter*>(rg_writer->column(col_id));
                float_writer->WriteBatch(1, nullptr, nullptr, &val);
                buffered_values_estimate[col_id] = float_writer->EstimatedBufferedValueBytes();


            } else if ("DOUBLE" == col_type) {
                double val = sqlite3_column_double(stmt, col_id);
                std::cout << "column name = " << col_name << ", type = " << col_type << ", val = " << val << std::endl;
                parquet::DoubleWriter* double_writer = static_cast<parquet::DoubleWriter*>(rg_writer->column(col_id));
                double_writer->WriteBatch(1, nullptr, nullptr, &val);
                buffered_values_estimate[col_id] = double_writer->EstimatedBufferedValueBytes();


            } else if ("BLOB" == col_type) {
                int blob_size = sqlite3_column_bytes(stmt, col_id);
                std::cout << "column name = " << col_name << ", type = " << col_type << ", val = " << blob_size << std::endl;
                if (blob_size > 0) {
                    const uint8_t *pBuffer = reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt, col_id));
                    parquet::ByteArrayWriter* ba_writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->column(col_id));
                    parquet::ByteArray ba_value;
                    ba_value.ptr = pBuffer;
                    ba_value.len = blob_size;
                    ba_writer->WriteBatch(1, nullptr, nullptr, &ba_value);
                    buffered_values_estimate[col_id] = ba_writer->EstimatedBufferedValueBytes();

                } else {
                    // blob_size is zero, what should we do?
                    //std::cout << col_name << " blob_size is zero" << std::endl;
                    blob_size = 1;
                    uint8_t local_buffer[blob_size];
                    //std::copy(pBuffer, pBuffer + blob_size, &buffer[0]);

                    parquet::ByteArrayWriter* ba_writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->column(col_id));
                    parquet::ByteArray ba_value;
                    ba_value.ptr = reinterpret_cast<const uint8_t*>(&local_buffer[0]);
                    ba_value.len = blob_size;
                    ba_writer->WriteBatch(1, nullptr, nullptr, &ba_value);
                    buffered_values_estimate[col_id] = ba_writer->EstimatedBufferedValueBytes();

                }
            } else if ("INTEGER" == col_type) {
                int val = sqlite3_column_int(stmt, col_id);
                std::cout << "column name = " << col_name << ", type = " << col_type << ", val = " << val << std::endl;
                parquet::Int32Writer* int32_writer = static_cast<parquet::Int32Writer*>(rg_writer->column(col_id));
                int32_writer->WriteBatch(1, nullptr, nullptr, &val);
                buffered_values_estimate[col_id] = int32_writer->EstimatedBufferedValueBytes();
            }
        }
    }

    sqlite3_finalize(stmt);
    sqlite3_close(pDb);

    return 1;
}

std::shared_ptr<parquet::schema::GroupNode> get_schema(string_map const &source_schema_map) {
    parquet::schema::NodeVector fields;
    fields.reserve(150);

//    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
//        string col_name = itr->first;
//        string col_type = itr->second;
//
//        parquet::Type::type type = parquet::Type::DOUBLE;
//        if ("BIGINT" == col_type) {
//            type = parquet::Type::INT64;
//        } else if ("FLOAT" == col_type) {
//            type = parquet::Type::FLOAT;
//        } else if ("DOUBLE" == col_type) {
//            type = parquet::Type::DOUBLE;
//        } else if ("BLOB" == col_type) {
//            type = parquet::Type::BYTE_ARRAY;
//        } else if ("INTEGER" == col_type) {
//            type = parquet::Type::INT32;
//        }
//
//        fields.push_back(parquet::schema::PrimitiveNode::Make(col_name,parquet::Repetition::REQUIRED,
//                                                              type,parquet::ConvertedType::NONE));
//    }

    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        string col_name = itr->first;
        string col_type = itr->second;

        if ("BIGINT" == col_type) {
            std::cout << "schema build col = " << col_name << ", type = " << col_type << std::endl;
            fields.push_back(parquet::schema::PrimitiveNode::Make(col_name,parquet::Repetition::REQUIRED,
                                                                  parquet::Type::INT64,parquet::ConvertedType::NONE));
        } else if ("FLOAT" == col_type) {
            std::cout << "schema build col = " << col_name << ", type = " << col_type << std::endl;
            fields.push_back(parquet::schema::PrimitiveNode::Make(col_name,parquet::Repetition::REQUIRED,
                                                                  parquet::Type::FLOAT,parquet::ConvertedType::NONE));
        } else if ("DOUBLE" == col_type) {
            std::cout << "schema build col = " << col_name << ", type = " << col_type << std::endl;
            fields.push_back(parquet::schema::PrimitiveNode::Make(col_name,parquet::Repetition::REQUIRED,
                                                                  parquet::Type::DOUBLE,parquet::ConvertedType::NONE));
        } else if ("BLOB" == col_type) {
            std::cout << "schema build col = " << col_name << ", type = " << col_type << std::endl;
            fields.push_back(parquet::schema::PrimitiveNode::Make(col_name,parquet::Repetition::REQUIRED,
                                                                  parquet::Type::BYTE_ARRAY,parquet::ConvertedType::NONE));
        } else if ("INTEGER" == col_type) {
            std::cout << "schema build col = " << col_name << ", type = " << col_type << std::endl;
            fields.push_back(parquet::schema::PrimitiveNode::Make(col_name,parquet::Repetition::REQUIRED,
                                                                  parquet::Type::INT32,parquet::ConvertedType::NONE));
        }
    }

//    fields.push_back(parquet::schema::PrimitiveNode::Make("Polarity",parquet::Repetition::REQUIRED,
//            parquet::Type::INT32,parquet::ConvertedType::INT_32));
//
//    fields.push_back(parquet::schema::PrimitiveNode::Make("defectKey$defectID", parquet::Repetition::REQUIRED,
//            parquet::Type::INT64,parquet::ConvertedType::INT_64));
//
//    fields.push_back(parquet::schema::PrimitiveNode::Make("PatchNoise", parquet::Repetition::REQUIRED,
//            parquet::Type::FLOAT,parquet::ConvertedType::NONE));
//
//    fields.push_back(parquet::schema::PrimitiveNode::Make("MaxDim", parquet::Repetition::REQUIRED,
//            parquet::Type::DOUBLE,parquet::ConvertedType::NONE));
//
//    fields.push_back(parquet::schema::PrimitiveNode::Make("iADCVector", parquet::Repetition::REQUIRED,
//            parquet::Type::BYTE_ARRAY,parquet::ConvertedType::NONE));


    std::cout << "number of node vector fields = " << fields.size() << std::endl;
    // Create a GroupNode named 'schema' using the primitive nodes defined above
    // This GroupNode is the root node of the schema tree
    return std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}

int process_each_data_batch(
        string_vec const &file_paths,
        string_map const &source_schema_map,
        data_sink_type memory_target,
        int thread_id,
        bool has_encrypt) {

    // output variable that holds total rows for this data batch
    int sum_num_rows_per_thread = 0;

    if (memory_target == Arrow) {
        std::vector<table_ptr> tables;
        tables.reserve(2100);
        int table_count = 0;

        // individual db file will be process in this loop
        for (auto file_path : file_paths) {
            table_ptr table;
            load_data_to_arrow(file_path, source_schema_map, &table);

            int current_rows = table->num_rows();
            sum_num_rows_per_thread += current_rows;
            table_count += 1;

//            if (current_rows <= 0) {
//                std::cout << "**** File has zero records: " << file_path << std::endl;
//            }

            std::cout << "IN LOOP: Before push_back, we have total rows:" << sum_num_rows_per_thread << ", table_count = " << table_count << ", current rows = " << current_rows << std::endl;

            tables.push_back(table);
//            if (tables.size() > 10) {
//                break;
//            }
        }
        //std::cout << "Total tables processed:  " << table_count << std::endl;

        arrow::Result<table_ptr> result = arrow::ConcatenateTables(tables);
        table_ptr result_table = result.ValueOrDie();
        std::cout << "After merging " << tables.size() << " tables, row size = " << result_table->num_rows() << ", thread id = " << thread_id << std::endl;

        write_parquet_file(*result_table, thread_id, source_schema_map, has_encrypt);

    } else if (memory_target == CppType) {
        // we just load data into cpp type in memory
        //std::cout << "Target memory is cpp standard types." << std::endl;
        for (auto file_path : file_paths) {
            sum_num_rows_per_thread += load_data_to_cpp_type(file_path);
        }
    } else if (memory_target == Parquet) {
        using FileClass = ::arrow::io::FileOutputStream;
        std::shared_ptr<FileClass> out_file;
        PARQUET_ASSIGN_OR_THROW(out_file, FileClass::Open(std::to_string(thread_id) + PARQUET));

        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::SNAPPY);
        std::shared_ptr<parquet::WriterProperties> props = builder.build();

        std::shared_ptr<parquet::schema::GroupNode> schema = get_schema(source_schema_map);

        // Create a ParquetFileWriter instance
        std::shared_ptr<parquet::ParquetFileWriter> file_writer = parquet::ParquetFileWriter::Open(out_file, schema, props);

        // Append a BufferedRowGroup to keep the RowGroup open until a certain size
        parquet::RowGroupWriter* rg_writer = file_writer->AppendBufferedRowGroup();

        int num_columns = file_writer->num_columns();
        std::vector<int64_t> buffered_values_estimate(num_columns, 0);

        for (auto file_path : file_paths) {
            int64_t estimated_bytes = 0;
            // Get the estimated size of the values that are not written to a page yet
            for (int n = 0; n < num_columns; n++) {
                estimated_bytes += buffered_values_estimate[n];
            }

            // We need to consider the compressed pages
            // as well as the values that are not compressed yet
            if ((rg_writer->total_bytes_written() + rg_writer->total_compressed_bytes() + estimated_bytes) > ROW_GROUP_SIZE) {
                rg_writer->Close();
                std::fill(buffered_values_estimate.begin(), buffered_values_estimate.end(), 0);
                rg_writer = file_writer->AppendBufferedRowGroup();
            }
            std::cout << "*******" << std::endl;
            load_data_to_parquet(file_path, source_schema_map, rg_writer, buffered_values_estimate);
            std::cout << "#######" << std::endl;
        }

        // Close the RowGroupWriter
        rg_writer->Close();
        // Close the ParquetFileWriter
        file_writer->Close();

        // Write the bytes to file
        DCHECK(out_file->Close().ok());
    }

    return sum_num_rows_per_thread;
}

int main(int argc, char** argv) {
    string dhl_name = "";
    string file_extension = "patch";
    int thread_count_per_node = 1;
    data_sink_type sink_target = Arrow;
    bool has_encrypt = true;

    // Print Help message
    if(argc == 2 && strcmp(argv[1], "-h") == 0) {
        std::cout << "Parameters List" << std::endl;
        std::cout << "1: name of DHL" << std::endl;
        std::cout << "2: source file types" << std::endl;
        std::cout << "3: thread counts, multiple of 6" << std::endl;
        std::cout << "4: detination types" << std::endl;
        std::cout << "5: turn on/off parquet encryption" << std::endl;
        std::cout << "sqlite-to-parquet test_dhl patch|patchAttr|patchAttr340M 6|12|24|48 arrow|cppType|parquet 1|0" << std::endl;
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
        }
    }

    if (argc > 5) {
        string encrypt = argv[5];

        if ("0" == encrypt) {
            has_encrypt = false;
        }
    }

    if (dhl_name == "") {
        std::cout << "Please specify a DHL name" << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "DHL: " << dhl_name << ", extension: " << file_extension
        << ", thread count per node: " << thread_count_per_node << ", Sink type: " << sink_target << std::endl;

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

    std::vector<std::future<int>> futures;

    // file_pathes_all_nodes has NODES_COUNT file_paths.  Each file_paths has all the files on that node
    int thread_id = 1;
    int total_row_count = 0;
    for (auto file_paths : file_paths_all_nodes) {
        auto vec_with_thread_count = split_vector(file_paths, thread_count_per_node);

        std::cout << "This node will have thread count = " << vec_with_thread_count.size() << std::endl;

        for (auto files : vec_with_thread_count) {
            std::cout << "This should be called just once" << std::endl;
            total_row_count += process_each_data_batch(files, source_schema_map, sink_target, thread_id++, has_encrypt);

//            std::future<int> future = std::async(std::launch::async, process_each_data_batch, files, source_schema_map, sink_target, thread_id++, has_encrypt);
//            futures.push_back(std::move(future));
        }
    }

    //std::cout << "All threads have been started...." << std::endl;

//    int total_row_count = 0;
//    for (auto&& future : futures) {
//        int count_per_thread = future.get();
//        total_row_count += count_per_thread;
//    }

    std::cout << "All threads finished their work.  The total row count is " << total_row_count << std::endl;

    auto end = std::chrono::steady_clock::now();
    elapsed_seconds = end - start;
    std::cout << "Total elapsed time: " << elapsed_seconds.count() << "s\n";

    return 0;
}

