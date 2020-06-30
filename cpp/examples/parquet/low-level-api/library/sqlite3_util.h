//
// Created by Wen Lai on 4/30/20.
//

#ifndef CPP_SQLITEUTIL_H
#define CPP_SQLITEUTIL_H

#include <fstream>
#include <iostream>

#include "common.h"
#include "sqlite3.h"

const string CHILD_DHL_KEY = "sAr5w3Vk5l";
const string DHL_KEY = "e9FkChw3xF";
const string QUERY_COLUMNS_FILE_NAME = "columns.txt";

class SqliteUtil {
public:
    string get_query_columns(string);
    int get_schema(string file_path, string_map& source_schema_map);
    void print_dhl_sqlite_schema(string_map const &source_schema_map);
};



//string CANONICAL_QUERY_STRING = get_query_columns(QUERY_COLUMNS_FILE_NAME);

#endif //CPP_SQLITEUTIL_H
