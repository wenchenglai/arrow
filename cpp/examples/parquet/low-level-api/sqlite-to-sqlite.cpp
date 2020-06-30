/*
 * 2020/06/24
 * Read multiple DHL files randomly, and save them into a few dhl files.
 * Random locator keys are generated by reading each file path and gets 1/10 of data
 *
 * This simulates the scenario where we select 100M from 300M, provided with locator keys
 *
 * We make one SQLite table as an Arrow RecordBatch, so the key challenge is to split
 * a giant Arrow Table into thousands of RecordBatch(es).  The next key challenge is sort
 * an Arrow Table with multiple columns
 *
 * usage:
 * local test:
 * ./sqlite-to-sqlite test_dhl /Users/wen/github/arrow/data/test_dirs /output_path
 *
 * */

#include <chrono>
#include <iostream>

#include "library/locator_key.h"
#include "library/sqlite_arrow.h"

int main(int argc, char** argv) {
    string dhl_name = "";
    string input_path = "/mnt/nodes/";
    string output_path = "output/";
    int num_output = 1;

    // Print Help message
    if(argc == 2 && strcmp(argv[1], "-h") == 0) {
        std::cout << "Parameters List" << std::endl;
        std::cout << "1: name of DHL" << std::endl;
        std::cout << "2: (optional) input path, default is /mnt/nodes/" << std::endl;
        std::cout << "3: (optional) output path, default is ""output/""" << std::endl;
        return 0;
    }

    if (argc > 1) {
        dhl_name = argv[1];
    }

    if (argc > 2) {
        input_path = argv[2];

        if (input_path.back() != '/')
            input_path += '/';
    }

    if (argc > 3) {
        output_path = argv[3];

        if (output_path.back() != '/')
            output_path += '/';
    }

    if (dhl_name == "") {
        std::cout << "Please specify a DHL name" << std::endl;
        return EXIT_FAILURE;
    }

    std::unique_ptr<SqliteArrow> io(new SqliteArrow());

    float size_ratio = 0.1;

    std::vector<uint64_t> locator_keys_for_random_selection = LocatorKey::GenerateRandomLocatorKeys(
            dhl_name, input_path, size_ratio);

    //std::vector<uint64_t> locator_keys_for_random_selection;

    std::cout << "Number of keys generated: " << locator_keys_for_random_selection.size() << std::endl;

    std::cout << "Start sorting the keys....." << std::endl;
    auto start = std::chrono::steady_clock::now();

    std::sort(locator_keys_for_random_selection.begin(), locator_keys_for_random_selection.end());

    auto stop1 = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = stop1 - start;
    std::cout << "Sorting finished. The elapsed time: " << elapsed_seconds.count() << " seconds" << std::endl;



//    for (uint64_t key : locator_keys_for_random_selection) {
//        std::cout << key << std::endl << std::endl;
//    }



    std::shared_ptr<arrow::Table> table =
            io->SQLiteToArrow(dhl_name, input_path, locator_keys_for_random_selection);

    auto stop2 = std::chrono::steady_clock::now();
    elapsed_seconds = stop2 - stop1;
    std::cout << "SQLite read operation is done using " << elapsed_seconds.count() << " seconds, table size = " << table->num_rows() << std::endl;


    std::cout << "Let's start saving arrow to multiple sqlite files..." << std::endl;
    std::vector<string> output_paths;

    for (int i = 0; i < num_output; i++) {
        output_paths.push_back(output_path + std::to_string(i) +".db");
    }

    io->arrow_to_sqlite_split(table, num_output, output_paths);

    auto stop3 = std::chrono::steady_clock::now();
    elapsed_seconds = stop3 - stop2;
    std::cout << "Finished writing data in " << elapsed_seconds.count() << " seconds, saved to sqlite at " << output_path << std::endl;

    elapsed_seconds = stop3 - start;
    std::cout << "Total elapsed time (after random locator keys generation) is " << elapsed_seconds.count() << " seconds." << std::endl;
    return 0;
}



