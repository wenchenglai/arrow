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

#define EXIT_ON_FAILURE(expr)                      \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      return EXIT_FAILURE;                         \
    }                                              \
  } while (0);

string get_query_columns(string);

enum memory_target_type {Arrow, CppType} memory_target;

const string QUERY_COLUMNS_FILE_NAME = "columns.txt";
const string PARQUET = ".parquet";
const int NODES_COUNT = 6;
const int PARQ_ROW_GROUP_SIZE = 1000;
const string col_encryp_key_id = "key1";
const string col_encryp_key = "9874567896123459";
const string footer_encryp_key_id = "key2";
const string footer_encryp_key = "9123856789712348";

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

    dhl_path = "/Volumes/remoteStorage/" + dhl_name;

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

int load_data_to_arrow(
        string file_path,
        string_map const &source_schema_map,
        table_ptr* table) {

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Create a hashtable for each column type builder.
    // The total count of all members of all hash tables should equal to the totol column count of the db.
    //BIGINT Int64Builder
    //DOUBLE DoubleBuilder
    //BLOB BinaryBuilder
    //FLOAT FloatingPointBuilder
    //INTEGER IntBuilder;

    std::unordered_map<string, std::shared_ptr<arrow::Int64Builder>> int64_builder_map;
    std::unordered_map<string, std::shared_ptr<arrow::DoubleBuilder>> double_builder_map;
    std::unordered_map<string, std::shared_ptr<arrow::BinaryBuilder>> binary_builder_map;
    std::unordered_map<string, std::shared_ptr<arrow::Int32Builder>> int_builder_map;

    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        if ("BIGINT" == itr->second) {
            int64_builder_map[itr->first] = std::make_shared<arrow::Int64Builder>(arrow::int64(), pool);

        } else if ("DOUBLE" == itr->second || "FLOAT" == itr->second) {
            //auto type = std::make_shared<arrow::Decimal128Type>(ARROW_DECIMAL_PRECISION, ARROW_DECIMAL_SCALE);
            double_builder_map[itr->first] = std::make_shared<arrow::DoubleBuilder>(pool);
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

            } else if (("DOUBLE" == col_type || "FLOAT" == col_type)
                       && double_builder_map.find(col_name) != double_builder_map.end()) {
                std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
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

// Write out the data as a Parquet file.  It'll also encrypt the output file.
void write_parquet_file(const arrow::Table& table, int node_id, string_map const &source_schema_map) {
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile,arrow::io::FileOutputStream::Open(std::to_string(node_id) + PARQUET));

    std::map<string, std::shared_ptr<parquet::ColumnEncryptionProperties>> encryption_cols;

    // we always encrypt all columns
    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        string column_name = itr->first;
        parquet::ColumnEncryptionProperties::Builder encryption_col_builder(column_name);
        encryption_col_builder.key(col_encryp_key)->key_id(col_encryp_key_id);
        encryption_cols[column_name] = encryption_col_builder.build();
    }
    parquet::FileEncryptionProperties::Builder file_encryption_builder(footer_encryp_key);

    parquet::WriterProperties::Builder builder;
    builder.encryption(file_encryption_builder.footer_key_metadata(footer_encryp_key_id)->encrypted_columns(encryption_cols)->build());

    builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();
    
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, PARQ_ROW_GROUP_SIZE, props));
}

int process_each_data_batch(
        string_vec const &file_paths,
        string_map const &source_schema_map,
        memory_target_type memory_target, int thread_id) {

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

            table_count += 1;

            if (current_rows <= 0) {
                std::cout << "**** File has zero records: " << file_path << std::endl;
            }

            sum_num_rows_per_thread += current_rows;

            std::cout << "IN LOOP: Before push_back, we have total rows:" << sum_num_rows_per_thread << ", table_count = " << table_count << ", current rows = " << current_rows << std::endl;

//            if (tables.size() < 412) {
//                tables.push_back(table);
//                std::cout << "IN LOOP: Vector size = " << tables.size() <<", capacity = " << tables.capacity() << std::endl;
//            } else {
//                std::cout << "IN LOOP: We didn't push_back for table # : " << table_count << std::endl;
//            }
            if (tables.size() > 100) {
                break;
            }
        }
        //std::cout << "Total tables processed:  " << table_count << std::endl;

        arrow::Result<table_ptr> result = arrow::ConcatenateTables(tables);
        table_ptr result_table = result.ValueOrDie();
        std::cout << "After merging " << tables.size() << " tables, row size = " << result_table->num_rows() << ", thread id = " << thread_id << std::endl;

        write_parquet_file(*result_table, thread_id, source_schema_map);

    } else if (memory_target == CppType) {
        // we just load data into cpp type in memory
        //std::cout << "Target memory is cpp standard types." << std::endl;
        for (auto file_path : file_paths) {
            sum_num_rows_per_thread += load_data_to_cpp_type(file_path);
        }
    }

    return sum_num_rows_per_thread;
}

int main(int argc, char** argv) {
    string dhl_name = "";
    string file_extension = "patch";
    int thread_count_per_node = 1;
    memory_target_type memory_target = Arrow;

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
            memory_target = CppType;
        }
    }

    if (dhl_name == "") {
        std::cout << "Please specify a DHL name" << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "DHL: " << dhl_name << ", extension: " << file_extension << ", thread count per node: " << thread_count_per_node << std::endl;

    if (memory_target == CppType) {
        std::cout << "Target memory is cpp standard types." << std::endl;
    }

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
    for (auto file_paths : file_paths_all_nodes) {
        auto vec_with_thread_count = split_vector(file_paths, thread_count_per_node);

        std::cout << "This node will have thread count = " << vec_with_thread_count.size() << std::endl;

        for (auto files : vec_with_thread_count) {
            std::future<int> future = std::async(std::launch::async, process_each_data_batch, files, source_schema_map, memory_target, thread_id++);
            futures.push_back(std::move(future));
        }
    }

    std::cout << "All threads have been started...." << std::endl;

    int total_row_count = 0;
    for (auto&& future : futures) {
        int count_per_thread = future.get();
        total_row_count += count_per_thread;
    }

    std::cout << "All threads finished their work.  The total row count is " << total_row_count << std::endl;

    auto end = std::chrono::steady_clock::now();
    elapsed_seconds = end - start;
    std::cout << "Total elapsed time: " << elapsed_seconds.count() << "s\n";

    return 0;
}

