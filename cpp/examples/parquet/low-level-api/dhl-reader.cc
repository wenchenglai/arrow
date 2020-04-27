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
#include <parquet/arrow/reader.h>
#include <parquet/api/reader.h>


#include "common.h"

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

const std::string PARQUET = ".parquet";

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

// open a parquet file (with encryption if needed) and save it into an arrow table
table_ptr read_parquet_file_into_arrow_table(string file_path, bool has_encrypt) {
    parquet::ReaderProperties reader_properties = parquet::default_reader_properties();

    if (has_encrypt) {
        std::shared_ptr<parquet::StringKeyIdRetriever> string_kr = std::make_shared<parquet::StringKeyIdRetriever>();
        string_kr->PutKey(footer_encryp_key_id, footer_encryp_key);
        string_kr->PutKey(col_encryp_key_id, col_encryp_key);
        std::shared_ptr<parquet::DecryptionKeyRetriever> kr = std::static_pointer_cast<parquet::StringKeyIdRetriever>(string_kr);

        parquet::FileDecryptionProperties::Builder file_decryption_builder;
        reader_properties.file_decryption_properties(file_decryption_builder.key_retriever(kr)->build());
    }

    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
                parquet::ParquetFileReader::OpenFile(file_path, false, reader_properties);

//        std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
//
//        // Get the number of RowGroups
//        int num_row_groups = file_metadata->num_row_groups();
//        //assert(num_row_groups == 1);
//
//        // Get the number of Columns
//        int num_columns = file_metadata->num_columns();
//        //assert(num_columns == 8);
//
//        std::cout << "file: " << file_path << ", num of row groups = " << num_row_groups << ", num_columns = " << num_columns << std::endl;
//
//        int64_t values_read = 0;
//        int64_t rows_read = 0;
//        int16_t definition_level;
//        int16_t repetition_level;
//
//        std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(0);
//
//        std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(0);
//
//        parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(column_reader.get());
//        // Read all the rows in the column
//        int i = 0;
//        while (int64_reader->HasNext()) {
//            int64_t value;
//            // Read one value at a time. The number of rows read is returned. values_read
//            // contains the number of non-null rows
//            rows_read = int64_reader->ReadBatch(10, &definition_level, &repetition_level, &value, &values_read);
//
//            std::cout << i << " deflevel: " << definition_level << ", replevel: " << repetition_level << ", value: " << value << ", valread: " << values_read << std::endl;
//
//            i++;
//        }

        // convert parquet reader to arrow reader, so we can get the arrow table
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        PARQUET_THROW_NOT_OK(parquet::arrow::FileReader::Make(::arrow::default_memory_pool(), std::move(parquet_reader), &arrow_reader));
        table_ptr table;
        PARQUET_THROW_NOT_OK(arrow_reader->ReadTable(&table));

        return table;
    } catch (const std::exception& e) {
        std::cerr << "Parquet read error: " << e.what() << std::endl;
    }
    return nullptr;
}

// Load each parquet file inside this folder path
// Each parquet file will spawn an independent thread
int load_data_from_folder(std::string input_folder_path, bool has_encrypt) {
    DIR *dir;
    struct dirent *ent;

    if ((dir = opendir (input_folder_path.c_str())) != NULL) {
        std::vector<std::future<std::shared_ptr<arrow::Table>>> futures;

        // print all the files and directories within directory
        while ((ent = readdir (dir)) != NULL) {
            std::string file_name = ent->d_name;

            // get rid of hidden . folder and other non-parquet files
            if (file_name.find(PARQUET) != std::string::npos) {
                int length = file_name.length();

                // make sure file name ends with .parquet
                if (file_name.substr(length - 8, length - 1) == PARQUET) {
                    std::string full_file_path = input_folder_path + file_name;
                    std::cout << "Reading parquet file: " << full_file_path << std::endl;

                    std::future<std::shared_ptr<arrow::Table>> future = std::async(
                            std::launch::async,
                            read_parquet_file_into_arrow_table,
                            full_file_path,
                            has_encrypt);
                    futures.push_back(std::move(future));
                }
            }
        }
        closedir (dir);

        std::cout << "All threads have been started...." << std::endl;

        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (auto&& future : futures) {
            std::shared_ptr<arrow::Table> table = future.get();
            tables.push_back(table);
            std::cout << "This table finished loading " << table->num_rows() << " total rows." << std::endl;
        }

        std::cout << "All thread are finished, we have " << tables.size() << " tables. Let's combine them into one table." << std::endl;
        auto start = std::chrono::steady_clock::now();

        arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::ConcatenateTables(tables);
        std::shared_ptr<arrow::Table> result_table = result.ValueOrDie();

        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed_seconds = end - start;
        std::cout << "Combining all tables takes: " << elapsed_seconds.count() << ".  The merged table has " << result_table->num_rows() << " rows and " << result_table->num_columns() << " columns." << std::endl;

        //std::cout << "Loaded " << row_count << " total rows in " << column_count << " columns." << std::endl;
        return EXIT_SUCCESS;
    } else {
        /* could not open directory */
        perror ("");
        return EXIT_FAILURE;
    }
}

int main(int argc, char** argv) {
    std::string input_folder_path = "";
    bool has_encrypt = true;

    // Print Help message
    if(argc == 2 && strcmp(argv[1], "-h") == 0) {
        std::cout << "Parameters List" << std::endl;
        std::cout << "1: folder path that contains one or more parquet files" << std::endl;
        std::cout << "2: use parquet encryption to read" << std::endl;
        std::cout << "dhl-reader parquet_folder 1|0" << std::endl;
        return 0;
    }

    if (argc > 1) {
        input_folder_path = argv[1];
    }

    if (argc > 2) {
        string encrypt = argv[2];

        if ("0" == encrypt) {
            has_encrypt = false;
        }
    }

    try {
        auto start = std::chrono::steady_clock::now();

        load_data_from_folder(input_folder_path, has_encrypt);

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
