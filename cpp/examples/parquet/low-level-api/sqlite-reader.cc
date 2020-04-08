// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <dirent.h>
#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <thread>

#include "reader_writer.h"
#include "sqlite3.h"
#include <arrow/api.h>
#include "arrow/util/decimal.h"
#include "arrow/testing/util.h"
//#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
//#include <parquet/arrow/writer.h>


#define EXIT_ON_FAILURE(expr)                      \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      return EXIT_FAILURE;                         \
    }                                              \
  } while (0);
/*
 * This example describes writing and reading Parquet Files in C++ and serves as a
 * reference to the API.
 * The file contains all the physical data types supported by Parquet.
 * This example uses the RowGroupWriter API that supports writing RowGroups optimized for
 *memory consumption
 **/

/* Parquet is a structured columnar file format
 * Parquet File = "Parquet data" + "Parquet Metadata"
 * "Parquet data" is simply a vector of RowGroups. Each RowGroup is a batch of rows in a
 * columnar layout
 * "Parquet Metadata" contains the "file schema" and attributes of the RowGroups and their
 * Columns
 * "file schema" is a tree where each node is either a primitive type (leaf nodes) or a
 * complex (nested) type (internal nodes)
 * For specific details, please refer the format here:
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 **/

//namespace fs = std::__fs::filesystem;

const char PARQUET_FILENAME[] = "dhl.parquet";
const std::string PARQUET = ".parquet";

class DhlRecord {
public:
    DhlRecord(int64_t defectKey$swathX, int64_t defectKey$swathY, int64_t defectKey$defectID) {
        this->defectKey$swathX = defectKey$swathX;
        this->defectKey$swathY = defectKey$swathY;
        this->defectKey$defectID = defectKey$defectID;
    }

    int64_t defectKey$swathX;
    int64_t defectKey$swathY;
    int64_t defectKey$defectID;
};

void print_data(int64_t rows_read, int16_t definition_level, int16_t repetition_level, int64_t value, int64_t values_read, int i) {
    std::cout << "rows_read = " << rows_read << std::endl;
    std::cout << "values_read = " << values_read << std::endl;
    std::cout << "value = " << value << std::endl;
    std::cout << "repetition_level = " << repetition_level << std::endl;
    std::cout << "definition_level = " << definition_level << std::endl;
    std::cout << "i = " << i << std::endl;
}

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

std::shared_ptr<arrow::Table> read_whole_file(std::string file_path) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(
            infile,
            arrow::io::ReadableFile::Open(file_path,
                                          arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    return table;
}

void read_whole_file_thread(std::string file_path) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(
            infile,
            arrow::io::ReadableFile::Open(file_path,
                                          arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    std::cout << "Loaded " << table->num_rows() << " total rows in " << table->num_columns() << " columns." << std::endl;
}

int load_data_from_folder(std::string input_folder_path) {
    DIR *dir;
    struct dirent *ent;
    if ((dir = opendir (input_folder_path.c_str())) != NULL) {
        //std::vector<std::shared_ptr<arrow::Table>> tables;

        std::vector<std::thread> threads;
        /* print all the files and directories within directory */
        while ((ent = readdir (dir)) != NULL) {
            std::string file_name = ent->d_name;

            // get rid of . folder and other non-parquet files
            if (file_name.find(PARQUET) != std::string::npos) {
                int length = file_name.length();

                // make sure file name ends with .parquet
                if (file_name.substr(length - 8, length - 1) == ".parquet") {
                    std::cout << file_name << std::endl;

                    threads.push_back(std::thread(read_whole_file_thread, input_folder_path + "/" + file_name));
                    //std::shared_ptr<arrow::Table> new_table = read_whole_file(input_folder_path + "/" + file_name);
                    //row_count += new_table->num_rows();
                    //column_count = new_table->num_columns();

                    //tables.push_back(new_table);
                }
            }
        }
        closedir (dir);

        for (auto& th : threads) th.join();
        //arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::ConcatenateTables(tables);
        //std::shared_ptr<arrow::Table> result_table = result.ValueOrDie();
        //std::cout << "Loaded " << result_table->num_rows() << " rows in " << result_table->num_columns() << " columns." << std::endl;
        //std::cout << "Loaded " << row_count << " total rows in " << column_count << " columns." << std::endl;

        //delete result;

    } else {
        /* could not open directory */
        perror ("");
        return EXIT_FAILURE;
    }
    return 0;
}

void tokenize(std::string const &str, const char delim, std::vector<std::string> &out)
{
    size_t start;
    size_t end = 0;

    while ((start = str.find_first_not_of(delim, end)) != std::string::npos)
    {
        end = str.find(delim, start);
        out.push_back(str.substr(start, end - start));
    }
}


int get_all_files_path(std::string dhl_name, std::string file_extension, std::vector<std::vector<std::string>> &file_paths_all_nodes) {
    // CONSTANTS declaration, could move else where for more flexibility
    int NODES_COUNT = 6;
    std::string DHL_ROOT_PATH = "/mnt/nodes/";

    if (!opendir(DHL_ROOT_PATH.c_str())) {
        DHL_ROOT_PATH = "/Users/wen/github/arrow/data/test_dirs/";
    }

    std::string DIE_ROW = "dierow_";
    std::string SWATH = "swath_";
    std::string CH0PATCH = "channel0." + file_extension;
    std::string CH1PATCH = "channel1." + file_extension;

    for (int i = 0; i < NODES_COUNT; i++) {
        std::vector<std::string> file_paths;
        file_paths.reserve(2100);

        std::string worker_node_path = "R" + std::to_string(i) + "C0S/";
        std::string dhl_path = DHL_ROOT_PATH + worker_node_path + dhl_name;

        std::cout << "Top Level Path = " << dhl_path << std::endl;

        DIR *dhl_dir = nullptr;
        if ((dhl_dir = opendir(dhl_path.c_str())) != NULL) {
            struct dirent *dhl_dir_item;

            while ((dhl_dir_item = readdir(dhl_dir)) != NULL) {
                if (dhl_dir_item->d_type == DT_DIR) {
                    std::string die_row_folder_name = dhl_dir_item->d_name;

                    //std::cout << "die_row_folder_name = " << die_row_folder_name << std::endl;

                    if (die_row_folder_name.find(DIE_ROW) != std::string::npos) {
                        //std::cout << "die_row_folder_name2 = " << die_row_folder_name << std::endl;
                        DIR *die_row_dir = nullptr;
                        std::string abs_die_row_path = dhl_path + "/" + die_row_folder_name;
                        if ((die_row_dir = opendir(abs_die_row_path.c_str())) != NULL) {
                            struct dirent *die_row_dir_item;
                            while ((die_row_dir_item = readdir(die_row_dir)) != NULL) {
                                if (die_row_dir_item->d_type == DT_DIR) {
                                    std::string swath_folder_name = die_row_dir_item->d_name;

                                    //std::cout << "swath_folder_name = " << swath_folder_name << std::endl;

                                    if (swath_folder_name.find(SWATH) != std::string::npos) {
                                        DIR *swath_dir = nullptr;
                                        std::string abs_swath_path = abs_die_row_path + "/" + swath_folder_name;
                                        if ((swath_dir = opendir(abs_swath_path.c_str())) != NULL) {
                                            struct dirent *swath_dir_item;
                                            while ((swath_dir_item = readdir(swath_dir)) != NULL) {
                                                if (swath_dir_item->d_type == DT_REG) {
                                                    std::string full_path_file_name = swath_dir_item->d_name;
                                                    const char delim = '/';
                                                    std::vector<std::string> fileTokens;

                                                    tokenize(full_path_file_name, delim, fileTokens);

                                                    std::string file_name_only = fileTokens.back();

                                                    //std::cout << "current file being considered = " << file_name_only << std::endl;

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
        } else {
            /* could not open directory */
            perror ("");
            return EXIT_FAILURE;
        }
        file_paths_all_nodes.push_back(file_paths);
    }

    return EXIT_SUCCESS;
}

void print_schema(std::unordered_map<std::string, std::string> const &source_schema_map) {
    std::cout << "******** Schema ******** = " << std::endl;
    int i = 1;
    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        std::cout << i++ << ": " << itr->first << "  " << itr->second << std::endl;
    }
}

arrow::Status VectorToColumnarTable(const std::vector<DhlRecord>& rows,
                                    std::shared_ptr<arrow::Table>* table) {

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    arrow::Int64Builder x_builder(pool);
    arrow::Int64Builder y_builder(pool);
    arrow::Int64Builder id_builder(pool);

    for (const DhlRecord& row : rows) {
        ARROW_RETURN_NOT_OK(x_builder.Append(row.defectKey$swathX));
        ARROW_RETURN_NOT_OK(y_builder.Append(row.defectKey$swathY));
        ARROW_RETURN_NOT_OK(id_builder.Append(row.defectKey$defectID));
    }

    std::shared_ptr<arrow::Array> x_array;
    ARROW_RETURN_NOT_OK(x_builder.Finish(&x_array));

    std::shared_ptr<arrow::Array> y_array;
    ARROW_RETURN_NOT_OK(y_builder.Finish(&y_array));

    std::shared_ptr<arrow::Array> id_array;
    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));


    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("defectKey$swathX", arrow::int64()), arrow::field("defectKey$swathY", arrow::int64()),
            arrow::field("defectKey$defectID", arrow::int64())};

    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    *table = arrow::Table::Make(schema, {x_array, y_array, id_array});

    return arrow::Status::OK();
}

int get_schema(std::string file_path, std::unordered_map<std::string, std::string>& source_schema_map) {
    sqlite3* pDb;
    int flags = (SQLITE_OPEN_READONLY);

    int bResult = sqlite3_open_v2(file_path.c_str(), &pDb, flags, NULL);

    if (SQLITE_OK != bResult) {
        sqlite3_close(pDb);
        std::cerr << "Cannot Open DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            std::string strMsg = sqlite3_errmsg(pDb);
            sqlite3_close_v2(pDb);
            pDb = nullptr;
        } else {
            std::cerr << "Unable to get DB handle" << std::endl;
        }
        return EXIT_FAILURE;
    }

    //std::string strKey = "sAr5w3Vk5l";
    std::string strKey = "e9FkChw3xF";
    bResult = sqlite3_key_v2(pDb, nullptr, strKey.c_str(), static_cast<int>(strKey.size()));

    if (SQLITE_OK != bResult) {
        sqlite3_close(pDb);
        std::cerr << "Cannot key the DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            std::string strMsg = sqlite3_errmsg(pDb);
            sqlite3_close_v2(pDb);
            pDb = nullptr;
            std::cerr << "SQLite Error Message: " << strMsg << std::endl;
        } else {
            std::cerr << "Unable to key the database" << std::endl;
        }

        return 0;
    }

    std::string query = "SELECT * FROM attribTable LIMIT 1;";

    sqlite3_stmt *stmt;
    bResult = sqlite3_prepare_v2(pDb, query.c_str(), -1, &stmt, NULL);

    if (SQLITE_OK != bResult) {
        sqlite3_finalize(stmt);
        sqlite3_close(pDb);
        std::cerr << "Cannot prepare statement from DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            std::string strMsg = sqlite3_errmsg(pDb);
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
        std::string col_name = sqlite3_column_name(stmt, i);
        std::string col_type = sqlite3_column_decltype(stmt, i);
        source_schema_map[col_name] = col_type;
    }
    return EXIT_SUCCESS;
}

int load_data_to_arrow(
        std::string file_path,
        std::unordered_map<std::string, std::string> const &source_schema_map,
        std::shared_ptr<arrow::Table>* table) {

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Create a hashtable for each column type builder.
    // The total count of all members of all hash tables should equal to the totol column count of the db.
    //BIGINT Int64Builder
    //DOUBLE DoubleBuilder
    //BLOB BinaryBuilder
    //FLOAT FloatingPointBuilder
    //INTEGER IntBuilder;

    std::unordered_map<std::string, std::shared_ptr<arrow::Int64Builder>> int64_builder_map;
    std::unordered_map<std::string, std::shared_ptr<arrow::DoubleBuilder>> double_builder_map;
    std::unordered_map<std::string, std::shared_ptr<arrow::BinaryBuilder>> binary_builder_map;
    std::unordered_map<std::string, std::shared_ptr<arrow::Int32Builder>> int_builder_map;

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
            std::string strMsg = sqlite3_errmsg(pDb);
            sqlite3_close_v2(pDb);
            pDb = nullptr;
        } else {
            std::cerr << "Unable to get DB handle" << std::endl;
        }
        return EXIT_FAILURE;
    }

    //std::string strKey = "sAr5w3Vk5l";
    std::string strKey = "e9FkChw3xF";
    bResult = sqlite3_key_v2(pDb, nullptr, strKey.c_str(), static_cast<int>(strKey.size()));

    if (SQLITE_OK != bResult) {
        sqlite3_close(pDb);
        std::cerr << "Cannot key the DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            std::string strMsg = sqlite3_errmsg(pDb);
            sqlite3_close_v2(pDb);
            pDb = nullptr;
            std::cerr << "SQLite Error Message: " << strMsg << std::endl;
        } else {
            std::cerr << "Unable to key the database" << std::endl;
        }

        return EXIT_FAILURE;
    }

    std::string query = "SELECT * FROM attribTable;";

    sqlite3_stmt *stmt;
    bResult = sqlite3_prepare_v2(pDb, query.c_str(), -1, &stmt, NULL);

    if (SQLITE_OK != bResult) {
        sqlite3_finalize(stmt);
        sqlite3_close(pDb);
        std::cerr << "Cannot prepare statement from DB: " << bResult << std::endl;

        if (nullptr != pDb) {
            std::string strMsg = sqlite3_errmsg(pDb);
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
            std::string col_name = sqlite3_column_name(stmt, i);
            std::string col_type = sqlite3_column_decltype(stmt, i);
            //std::cout << i << " col_name = " << col_name << ", col_type = " << col_type << std::endl;

            if ("BIGINT" == col_type && int64_builder_map.find(col_name) != int64_builder_map.end()) {
                std::shared_ptr<arrow::Int64Builder> builder = int64_builder_map[col_name];
                builder->Append(sqlite3_column_int64(stmt, i));

            } else if (("DOUBLE" == col_type || "FLOAT" == col_type)
            && double_builder_map.find(col_name) != double_builder_map.end()) {
                std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
                double val = sqlite3_column_double(stmt, i);
                builder->Append(val);

            } else if ("BLOB" == col_type && binary_builder_map.find(col_name) != binary_builder_map.end()) {
                int blob_size = sqlite3_column_bytes(stmt, i);
                if (blob_size > 0) {
                    const uint8_t *pBuffer = reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt, i));
                    //uint8_t buffer[blob_size];
                    //std::copy(pBuffer, pBuffer + blob_size, &buffer[0]);

                    std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
                    builder->Append(pBuffer, blob_size);
                }
            } else if ("INTEGER" == col_type && int_builder_map.find(col_name) != int_builder_map.end()) {
                std::shared_ptr<arrow::Int32Builder> builder = int_builder_map[col_name];
                builder->Append(sqlite3_column_int(stmt, i));
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

//    for (auto itr = int64_builder_map.begin(); itr != int64_builder_map.end(); itr++) {
//        std::cout << "For column: " << itr->first << ", we have " << int64_builder_map[itr->first]->length() << std::endl;
//    }

    // Two tasks are accomplished in here:
    // 1. create arrow array for each column.  The whole arrays object will be used to create an Arrow Table
    // 2. create a vector of Field (schema) along the way
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        std::string col_name = itr->first;
        std::string col_type = itr->second;

        std::shared_ptr<arrow::Array> array;
        if ("BIGINT" == col_type && int64_builder_map.find(col_name) != int64_builder_map.end()) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::int64()));

            std::shared_ptr<arrow::Int64Builder> builder = int64_builder_map[col_name];
            builder->Finish(&array);

        } else if (("DOUBLE" == col_type || "FLOAT" == col_type)
                   && double_builder_map.find(col_name) != double_builder_map.end()) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::float64()));

            std::shared_ptr<arrow::DoubleBuilder> builder = double_builder_map[col_name];
            builder->Finish(&array);

        } else if ("BLOB" == col_type && binary_builder_map.find(col_name) != binary_builder_map.end()) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::binary()));

            std::shared_ptr<arrow::BinaryBuilder> builder = binary_builder_map[col_name];
            builder->Finish(&array);

        } else if ("INTEGER" == col_type && int_builder_map.find(col_name) != int_builder_map.end()) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::int32()));

            std::shared_ptr<arrow::Int32Builder> builder = int_builder_map[col_name];
            builder->Finish(&array);
        }

        arrays.emplace_back(array);
    }

    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    *table = arrow::Table::Make(schema, arrays);

    //std::cout << "Arrows Loaded " << table->num_rows() << " total rows in " << table->num_columns() << " columns." << std::endl;

    return 0;
}

void process_each_node(std::vector<std::string> const &file_paths, std::unordered_map<std::string, std::string> const &source_schema_map) {
    std::vector<std::shared_ptr<arrow::Table>> tables;
    tables.reserve(2100);

    for (auto file_path : file_paths) {
        std::shared_ptr<arrow::Table> table;
        load_data_to_arrow(file_path, source_schema_map, &table);
        tables.push_back(table);
        // break;
    }

    arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::ConcatenateTables(tables);
    std::shared_ptr<arrow::Table> result_table = result.ValueOrDie();
    std::cout << "After merging " << tables.size() << " tables, row size = " << result_table->num_rows() << ", columns size = " << result_table->num_columns() << std::endl;
}

int main(int argc, char** argv) {
    std::string dhl_name = "";
    std::string file_extension = "patch";
    if (argc > 1) {
        dhl_name = argv[1];
    }

    if (argc > 2) {
        file_extension = argv[2];
    }

    if (dhl_name == "") {
        std::cout << "Please specify a DHL name" << std::endl;
        return EXIT_FAILURE;
    }

    auto start = std::chrono::steady_clock::now();

    // we will create one vector of patch files per node
    std::vector<std::vector<std::string>> file_paths_all_nodes;
    get_all_files_path(dhl_name, file_extension, file_paths_all_nodes);

    auto stop1 = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = stop1 - start;
    std::cout << "Patch file paths collection finished. The elapsed time: " << elapsed_seconds.count() << " seconds\n";

    for (auto file_paths: file_paths_all_nodes) {
        std::cout << "Files count per node = " << file_paths.size() << std::endl;
    }

    // create data source db schema, as it's needed for arrow table creation
    // it's better to get schema here, because every thread need the same schema object
    std::unordered_map<std::string, std::string> source_schema_map;
    get_schema(file_paths_all_nodes.front().front(), source_schema_map);
    //print_schema(source_schema_map);

    // thread list to join later
    std::vector<std::thread> threads;

    for (auto file_paths : file_paths_all_nodes) {
        std::cout << "Creating a new thread to process files per node...." << std::endl;
        threads.push_back(std::thread(process_each_node, file_paths, source_schema_map));
        //break;
    }

    std::cout << "All threads have been started...." << std::endl;

    for (auto& th : threads) {
        th.join();
    }
    std::cout << "All threads finished their work." << std::endl;

    auto end = std::chrono::steady_clock::now();
    elapsed_seconds = end - start;
    std::cout << "Total elapsed time: " << elapsed_seconds.count() << "s\n";

    return 0;
}

