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
//#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
//#include <parquet/arrow/writer.h>

using arrow::Int64Builder;

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

void load_data_single_file() {
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(PARQUET_FILENAME, false);

    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

    int num_row_groups = file_metadata->num_row_groups();

    //print_metadata(file_metadata);

    const int OUTPUT_SIZE = 1;
    num_row_groups = 0;

    for (int r = 0; r < num_row_groups; ++r) {
        // Get the RowGroup Reader
        std::shared_ptr <parquet::RowGroupReader> row_group_reader =
                parquet_reader->RowGroup(r);

        int64_t values_read = 0;
        int64_t rows_read = 0;
        int16_t definition_level;
        int16_t repetition_level;
        int i;
        int column_index = 0;
        std::shared_ptr <parquet::ColumnReader> column_reader;

        ARROW_UNUSED(rows_read);

        column_reader = row_group_reader->Column(column_index);
        parquet::Int64Reader *int64_reader = static_cast<parquet::Int64Reader *>(column_reader.get());

        i = 1;
        while (int64_reader->HasNext()) {
            int64_t value;
            // Read one value at a time. The number of rows read is returned. values_read
            // contains the number of non-null rows
            rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);

            if (i <= OUTPUT_SIZE) {
                print_data(rows_read, definition_level, repetition_level, value, values_read, i);
            }
            i++;
        }

        column_index++;

        column_reader = row_group_reader->Column(column_index);
        int64_reader = static_cast<parquet::Int64Reader *>(column_reader.get());

        i = 1;
        while (int64_reader->HasNext()) {
            int64_t value;
            // Read one value at a time. The number of rows read is returned. values_read
            // contains the number of non-null rows
            rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);

            if (i <= OUTPUT_SIZE) {
                print_data(rows_read, definition_level, repetition_level, value, values_read, i);
            }
            i++;
        }

        column_index++;

//
//            // Get the Column Reader for the boolean column
//            column_reader = row_group_reader->Column(0);
//            parquet::BoolReader* bool_reader =
//                    static_cast<parquet::BoolReader*>(column_reader.get());
//
//            // Read all the rows in the column
//            i = 0;
//            while (bool_reader->HasNext()) {
//                bool value;
//                // Read one value at a time. The number of rows read is returned. values_read
//                // contains the number of non-null rows
//                rows_read = bool_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
//                // Ensure only one value is read
//                assert(rows_read == 1);
//                // There are no NULL values in the rows written
//                assert(values_read == 1);
//                // Verify the value written
//                bool expected_value = ((i % 2) == 0) ? true : false;
//                assert(value == expected_value);
//                i++;
//            }
//
//            // Get the Column Reader for the Int32 column
//            column_reader = row_group_reader->Column(1);
//            parquet::Int32Reader* int32_reader =
//                    static_cast<parquet::Int32Reader*>(column_reader.get());
//            // Read all the rows in the column
//            i = 0;
//            while (int32_reader->HasNext()) {
//                int32_t value;
//                // Read one value at a time. The number of rows read is returned. values_read
//                // contains the number of non-null rows
//                rows_read = int32_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
//                // Ensure only one value is read
//                assert(rows_read == 1);
//                // There are no NULL values in the rows written
//                assert(values_read == 1);
//                // Verify the value written
//                assert(value == i);
//                i++;
//            }
//
//            // Get the Column Reader for the Int64 column
//            column_reader = row_group_reader->Column(2);
//            parquet::Int64Reader* int64_reader =
//                    static_cast<parquet::Int64Reader*>(column_reader.get());
//            // Read all the rows in the column
//            i = 0;
//            while (int64_reader->HasNext()) {
//                int64_t value;
//                // Read one value at a time. The number of rows read is returned. values_read
//                // contains the number of non-null rows
//                rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level,
//                                                    &value, &values_read);
//                // Ensure only one value is read
//                assert(rows_read == 1);
//                // There are no NULL values in the rows written
//                assert(values_read == 1);
//                // Verify the value written
//                int64_t expected_value = i * 1000 * 1000;
//                expected_value *= 1000 * 1000;
//                assert(value == expected_value);
//                if ((i % 2) == 0) {
//                    assert(repetition_level == 1);
//                } else {
//                    assert(repetition_level == 0);
//                }
//                i++;
//            }
//
//            // Get the Column Reader for the Int96 column
//            column_reader = row_group_reader->Column(3);
//            parquet::Int96Reader* int96_reader =
//                    static_cast<parquet::Int96Reader*>(column_reader.get());
//            // Read all the rows in the column
//            i = 0;
//            while (int96_reader->HasNext()) {
//                parquet::Int96 value;
//                // Read one value at a time. The number of rows read is returned. values_read
//                // contains the number of non-null rows
//                rows_read = int96_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
//                // Ensure only one value is read
//                assert(rows_read == 1);
//                // There are no NULL values in the rows written
//                assert(values_read == 1);
//                // Verify the value written
//                parquet::Int96 expected_value;
//                ARROW_UNUSED(expected_value); // prevent warning in release build
//                expected_value.value[0] = i;
//                expected_value.value[1] = i + 1;
//                expected_value.value[2] = i + 2;
//                for (int j = 0; j < 3; j++) {
//                    assert(value.value[j] == expected_value.value[j]);
//                }
//                i++;
//            }
//
//            // Get the Column Reader for the Float column
//            column_reader = row_group_reader->Column(4);
//            parquet::FloatReader* float_reader =
//                    static_cast<parquet::FloatReader*>(column_reader.get());
//            // Read all the rows in the column
//            i = 0;
//            while (float_reader->HasNext()) {
//                float value;
//                // Read one value at a time. The number of rows read is returned. values_read
//                // contains the number of non-null rows
//                rows_read = float_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
//                // Ensure only one value is read
//                assert(rows_read == 1);
//                // There are no NULL values in the rows written
//                assert(values_read == 1);
//                // Verify the value written
//                float expected_value = static_cast<float>(i) * 1.1f;
//                assert(value == expected_value);
//                i++;
//            }
//
//            // Get the Column Reader for the Double column
//            column_reader = row_group_reader->Column(5);
//            parquet::DoubleReader* double_reader =
//                    static_cast<parquet::DoubleReader*>(column_reader.get());
//            // Read all the rows in the column
//            i = 0;
//            while (double_reader->HasNext()) {
//                double value;
//                // Read one value at a time. The number of rows read is returned. values_read
//                // contains the number of non-null rows
//                rows_read = double_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
//                // Ensure only one value is read
//                assert(rows_read == 1);
//                // There are no NULL values in the rows written
//                assert(values_read == 1);
//                // Verify the value written
//                double expected_value = i * 1.1111111;
//                assert(value == expected_value);
//                i++;
//            }
//
//            // Get the Column Reader for the ByteArray column
//            column_reader = row_group_reader->Column(6);
//            parquet::ByteArrayReader* ba_reader =
//                    static_cast<parquet::ByteArrayReader*>(column_reader.get());
//            // Read all the rows in the column
//            i = 0;
//            while (ba_reader->HasNext()) {
//                parquet::ByteArray value;
//                // Read one value at a time. The number of rows read is returned. values_read
//                // contains the number of non-null rows
//                rows_read =
//                        ba_reader->ReadBatch(1, &definition_level, nullptr, &value, &values_read);
//                // Ensure only one value is read
//                assert(rows_read == 1);
//                // Verify the value written
//                char expected_value[FIXED_LENGTH] = "parquet";
//                ARROW_UNUSED(expected_value); // prevent warning in release build
//                expected_value[7] = static_cast<char>('0' + i / 100);
//                expected_value[8] = static_cast<char>('0' + (i / 10) % 10);
//                expected_value[9] = static_cast<char>('0' + i % 10);
//                if (i % 2 == 0) {  // only alternate values exist
//                    // There are no NULL values in the rows written
//                    assert(values_read == 1);
//                    assert(value.len == FIXED_LENGTH);
//                    assert(memcmp(value.ptr, &expected_value[0], FIXED_LENGTH) == 0);
//                    assert(definition_level == 1);
//                } else {
//                    // There are NULL values in the rows written
//                    assert(values_read == 0);
//                    assert(definition_level == 0);
//                }
//                i++;
//            }
//
//            // Get the Column Reader for the FixedLengthByteArray column
//            column_reader = row_group_reader->Column(7);
//            parquet::FixedLenByteArrayReader* flba_reader =
//                    static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());
//            // Read all the rows in the column
//            i = 0;
//            while (flba_reader->HasNext()) {
//                parquet::FixedLenByteArray value;
//                // Read one value at a time. The number of rows read is returned. values_read
//                // contains the number of non-null rows
//                rows_read = flba_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
//                // Ensure only one value is read
//                assert(rows_read == 1);
//                // There are no NULL values in the rows written
//                assert(values_read == 1);
//                // Verify the value written
//                char v = static_cast<char>(i);
//                char expected_value[FIXED_LENGTH] = {v, v, v, v, v, v, v, v, v, v};
//                assert(memcmp(value.ptr, &expected_value[0], FIXED_LENGTH) == 0);
//                i++;
//            }
    }
}

int get_all_files_path(std::string dhl_name, std::vector<std::vector<std::string>> &file_paths_all_nodes) {
    // CONSTANTS declaration, could move else where for more flexibility
    int NODES_COUNT = 6;
    std::string DHL_ROOT_PATH = "/mnt/nodes/";
    DHL_ROOT_PATH = "/Users/wen/github/arrow/data/test_dirs/";
    std::string DIE_ROW = "dierow_";
    std::string SWATH = "swath_";
    std::string CH0PATCH = "channel0.patch";
    std::string CH1PATCH = "channel1.patch";

    for (int i = 0; i < NODES_COUNT; i++) {
        std::vector<std::string> file_paths;

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
                                                    std::string file_name = swath_dir_item->d_name;

                                                    if (file_name.find(CH0PATCH) != std::string::npos
                                                        || file_name.find(CH1PATCH) != std::string::npos) {
                                                        file_paths.push_back(abs_swath_path + "/" + file_name);
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

    return 1;
}

arrow::Status VectorToColumnarTable(const std::vector<DhlRecord>& rows,
                                    std::shared_ptr<arrow::Table>* table) {
    // The builders are more efficient using
    // arrow::jemalloc::MemoryPool::default_pool() as this can increase the size of
    // the underlying memory regions in-place. At the moment, arrow::jemalloc is only
    // supported on Unix systems, not Windows.
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    Int64Builder x_builder(pool);
    Int64Builder y_builder(pool);
    Int64Builder id_builder(pool);

    // Now we can loop over our existing data and insert it into the builders. The
    // `Append` calls here may fail (e.g. we cannot allocate enough additional memory).
    // Thus we need to check their return values. For more information on these values,
    // check the documentation about `arrow::Status`.
    for (const DhlRecord& row : rows) {
        ARROW_RETURN_NOT_OK(x_builder.Append(row.defectKey$swathX));
        ARROW_RETURN_NOT_OK(y_builder.Append(row.defectKey$swathY));
        ARROW_RETURN_NOT_OK(id_builder.Append(row.defectKey$defectID));
    }

    // At the end, we finalise the arrays, declare the (type) schema and combine them
    // into a single `arrow::Table`:
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

    // The final `table` variable is the one we then can pass on to other functions
    // that can consume Apache Arrow memory structures. This object has ownership of
    // all referenced data, thus we don't have to care about undefined references once
    // we leave the scope of the function building the table and its underlying arrays.
    *table = arrow::Table::Make(schema, {x_array, y_array, id_array});

    return arrow::Status::OK();
}

int load_data_to_arrow(std::string file_path) {
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
        return 0;
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

        return 0;
    }

    std::vector<DhlRecord> records;

    while ((bResult = sqlite3_step(stmt)) == SQLITE_ROW) {
        int col_index = 0;
        int x = sqlite3_column_int64(stmt, col_index++);
        int y = sqlite3_column_int64(stmt, col_index++);
        int id = sqlite3_column_int64(stmt, col_index++);

        records.emplace_back(x, y, id);
        //const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col_index++));
        //const char* number = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        //long value = static_cast<long>(sqlite3_column_int64(stmt, col_index++));
        //std::cout << "id = " << id << ", x = " << x << std::endl;
    }
    if (bResult != SQLITE_DONE) {
        std::cerr << "SELECT failed: " << sqlite3_errmsg(pDb) << std::endl;
        // if you return/throw here, don't forget the finalize
    }

    sqlite3_finalize(stmt);
    sqlite3_close(pDb);

//    for (DhlRecord record : records) {
//        std::cout << "Record obj: x = " << record.defectKey$swathX << ".  y = " << record.defectKey$swathY << ". id = " << record.defectKey$defectID << std::endl;
//    }
    // Now we have a list of records, next we populate an arrow table
    std::shared_ptr<arrow::Table> table;
    EXIT_ON_FAILURE(VectorToColumnarTable(records, &table));
    //VectorToColumnarTable(records, &table);

    //std::cout << "Arrows Loaded " << table->num_rows() << " total rows in " << table->num_columns() << " columns." << std::endl;
    return 0;
}

void process_each_node(std::vector<std::string> const &file_paths) {
    for (auto file_path : file_paths) {
        load_data_to_arrow(file_path);
    }
}

int main(int argc, char** argv) {
    std::string dhl_name = "";
    if (argc > 1) {
        dhl_name = argv[1];
    }

    auto start = std::chrono::steady_clock::now();

    // we will create one vector per node
    std::vector<std::vector<std::string>> file_paths_all_nodes;
    get_all_files_path(dhl_name, file_paths_all_nodes);

    int node_size = file_paths_all_nodes.size();
    std::cout << "Node Count  = " << node_size << std::endl;

    for (auto file_paths: file_paths_all_nodes) {
        std::cout << "Node List size = " << file_paths.size() << std::endl;
        //std::cout << "first element = " << file_paths.front() << std::endl;
        //std::cout << "last element = " << file_paths.back() << std::endl;
    }

    std::vector<std::thread> threads;

    for (auto file_paths : file_paths_all_nodes) {
        threads.push_back(std::thread(process_each_node, file_paths));
    }

    for (auto& th : threads) th.join();

    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

    return 0;
//    try {
//        auto start = std::chrono::steady_clock::now();
//
//        load_data_from_folder(input_folder_path);
//
//        auto end = std::chrono::steady_clock::now();
//        std::chrono::duration<double> elapsed_seconds = end - start;
//        std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";
//
//    } catch (const std::exception& e) {
//        std::std::cerr << "Parquet read error: " << e.what() << std::endl;
//        return -1;
//    }
//
//    std::cout << "Parquet Reading Completed!" << std::endl;
}

