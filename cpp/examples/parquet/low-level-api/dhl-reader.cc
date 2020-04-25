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

std::shared_ptr<arrow::Table> read_parquet_file_into_arrow_table(std::string file_path) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile,arrow::io::ReadableFile::Open(file_path, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    return table;
}

int load_data_from_folder(std::string input_folder_path) {
    DIR *dir;
    struct dirent *ent;

    if ((dir = opendir (input_folder_path.c_str())) != NULL) {
        //int row_count = 0;
        //int column_count = 0;

        //std::vector<std::thread> threads;
        std::vector<std::future<std::shared_ptr<arrow::Table>>> futures;

        /* print all the files and directories within directory */
        while ((ent = readdir (dir)) != NULL) {
            std::string file_name = ent->d_name;

            // get rid of . folder and other non-parquet files
            if (file_name.find(PARQUET) != std::string::npos) {
                int length = file_name.length();

                // make sure file name ends with .parquet
                if (file_name.substr(length - 8, length - 1) == PARQUET) {
                    std::string full_file_path = input_folder_path + file_name;
                    std::cout << "Reading parquet file: " << full_file_path << std::endl;

                    std::future<std::shared_ptr<arrow::Table>> future = std::async(std::launch::async, read_parquet_file_into_arrow_table, full_file_path);
                    futures.push_back(std::move(future));

                    //threads.push_back(std::thread(read_whole_file_thread, input_folder_path + "/" + file_name));

                    //row_count += new_table->num_rows();
                    //column_count = new_table->num_columns();

                    //tables.push_back(new_table);
                }
            }
        }
        closedir (dir);

        std::cout << "All threads have been started...." << std::endl;

//        for (auto& th : threads) {
//            th.join();
//        }

        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (auto&& future : futures) {
            std::shared_ptr<arrow::Table> table = future.get();
            tables.push_back(table);
            std::cout << "This table loaded " << table->num_rows() << " total rows." << std::endl;
        }

        std::cout << "All thread are finished, let's combine them into one table." << std::endl;
        auto start = std::chrono::steady_clock::now();

        arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::ConcatenateTables(tables);
        std::shared_ptr<arrow::Table> result_table = result.ValueOrDie();

        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed_seconds = end - start;
        std::cout << "Combining all tables takes: " << elapsed_seconds.count() << ".  The table has " << result_table->num_rows() << " rows and " << result_table->num_columns() << " columns." << std::endl;

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
