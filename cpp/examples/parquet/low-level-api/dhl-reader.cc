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
#include <arrow/api.h>
//#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
//#include <parquet/arrow/writer.h>

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

constexpr int NUM_ROWS_PER_ROW_GROUP = 500;
const char PARQUET_FILENAME[] = "dhl.parquet";
const std::string PARQUET = ".parquet";

class DhlRecord {
public:
    DhlRecord(int64_t defectKey$swathX, int64_t defectKey$swathY, int64_t defectKey$defectID) {
        this->defectKey$swathX = defectKey$swathX;
        this->defectKey$swathY = defectKey$swathY;
        this->defectKey$defectID = defectKey$defectID;
    }
private:
    int64_t defectKey$swathX;
    int64_t defectKey$swathY;
    int64_t defectKey$defectID;
};

class Dhl {
public:
    void push(DhlRecord record) {
        records.push_back(record);
    }
private:
    std::vector<DhlRecord> records;
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
        int row_count = 0;
        int column_count = 0;

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

int main(int argc, char** argv) {
    std::string input_folder_path = "";
    if (argc > 1) {
        input_folder_path = argv[1];
    }
    //std::cout << "argc = " << argc << ", argv = " << input_folder_path << std::endl;

    try {
        auto start = std::chrono::steady_clock::now();

        load_data_from_folder(input_folder_path);

        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed_seconds = end - start;
        std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

    } catch (const std::exception& e) {
        std::cerr << "Parquet read error: " << e.what() << std::endl;
        return -1;
    }

    std::cout << "Parquet Reading Completed!" << std::endl;

    return 0;
}
