/*
 * 2020/06/17
 * Read multiple DHL files randomly, and output to multiple dhlx files
 *
 * usage:
 * local test:
 * ./sqlite-to-dhlx test_dhl /Users/wen/github/arrow/data/test_dirs output_folder 2
 *
 * */

#include <iostream>

#include "sqlite_arrow.h"

int main(int argc, char** argv) {
    string dhl_name = "";
    string input_path = "/mnt/nodes/";
    string output_path = "";
    int num_output = 1;

    // Print Help message
    if(argc == 2 && strcmp(argv[1], "-h") == 0) {
        std::cout << "Parameters List" << std::endl;
        std::cout << "1: name of DHL" << std::endl;
        std::cout << "2: (optional) input path, default is /mnt/nodes/" << std::endl;
        std::cout << "3: (optional) output path, default is current directory" << std::endl;
        std::cout << "4: (optional) number of output files, default is 1" << std::endl;
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

    if (argc > 4) {
        num_output = std::stoi(argv[4]);
    }

    if (dhl_name == "") {
        std::cout << "Please specify a DHL name" << std::endl;
        return EXIT_FAILURE;
    }

    table_ptr table;

    SqliteArrow* io = new SqliteArrow();

    io->sqlite_to_arrow(dhl_name, input_path, true, &table);

    std::cout << "TESTER: Read operation is done, table size = " << table->num_rows() << std::endl;

    //std::cout << "TESTER: Let's start saving arrow to sqlite..." << std::endl;
    //io->arrow_to_sqlite(table, output_path + "1.db");

    std::cout << "TESTER: Let's start saving arrow to multiple sqlite files..." << std::endl;
    std::vector<string> output_paths;

    for (int i = 0; i < num_output; i++) {
        output_paths.push_back(output_path + std::to_string(i) +".db");
    }

    io->arrow_to_sqlite_split(table, num_output, output_paths);

    std::cout << "TESTER: Finished saving data to sqlite at " << output_path << std::endl;

    return 0;
}

