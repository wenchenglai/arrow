//
// Created by Wen Lai on 4/26/20.
//

#ifndef CPP_COMMON_H
#define CPP_COMMON_H

typedef std::shared_ptr<arrow::Table> table_ptr;
typedef std::string string;
typedef std::unordered_map<std::string, std::string> string_map;
typedef std::vector<std::string> string_vec;

const string col_encryp_key_id = "key1";
const string col_encryp_key = "9874567896123459";
const string footer_encryp_key_id = "key2";
const string footer_encryp_key = "9123856789712348";

#endif //CPP_COMMON_H
