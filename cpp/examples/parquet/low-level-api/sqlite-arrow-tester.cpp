#include <iostream>

#include "sqlite_arrow.h"

int main(int argc, char** argv) {
    string dhl_name = "";
    string input_path = "/mnt/nodes/";
    string output_path = "/arrow-sqlite-output/output.sqlite.patch";

    // Print Help message
    if(argc == 2 && strcmp(argv[1], "-h") == 0) {
        std::cout << "Parameters List" << std::endl;
        std::cout << "1: name of DHL" << std::endl;
        std::cout << "2: input path" << std::endl;
        std::cout << "3: output path" << std::endl;
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

        if (output_path.back() == '/')
            output_path += "output.sqlite.patch";
    }

    if (dhl_name == "") {
        std::cout << "Please specify a DHL name" << std::endl;
        return EXIT_FAILURE;
    }

    table_ptr table;

    SqliteArrow* io = new SqliteArrow();

    io->sqlite_to_arrow(dhl_name, input_path, &table);

    std::cout << "TESTER: Read operation is done, table size = " << table->num_rows() << std::endl;

    std::cout << "TESTER: Let's start saving arrow to sqlite..." << std::endl;
    io->arrow_to_sqlite(table, output_path);

//    std::cout << "TESTER: Let's start saving arrow to multiple sqlite files..." << std::endl;
//    std::vector<string> output_paths;
//    output_paths.push_back("output.sqlite.patch");
//    output_paths.push_back("output.sqlite2.patch");
//    io->arrow_to_sqlite_split(table, 2, output_paths);

    std::cout << "TESTER: Finished saving data to sqlite at" << output_path << std::endl;

    return 0;
}


