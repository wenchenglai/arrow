#ifndef ARROW_SQLITE_ARROW_H
#define ARROW_SQLITE_ARROW_H

#include "sqlite3_util.h"

void tokenize(string const &str, const char delim, string_vec &out);

class SqliteArrow {
public:
    /// \brief Convert SQLite data in disk to Arrow Table in memory
    /// \param[string] dhl_name the position of the first element in the constructed
    /// slice
    /// \param[string] input_path the length of the slice. If there are not enough
    /// elements in the array, the length will be adjusted accordingly
    ///
    /// \param[std::shared_ptr<arrow::Table>*] input_path the length of the slice. If there are not enough
    /// elements in the array, the length will be adjusted accordingly
    ///
    /// \return status indicate EXIT_SUCCESS or EXIT_FAILURE
    int sqlite_to_arrow(std::string dhl_name, std::string input_path, std::shared_ptr<arrow::Table>* table);
    int arrow_to_sqlite(std::shared_ptr<arrow::Table> table, std::string output_file_path);
    int arrow_to_sqlite_split(std::shared_ptr<arrow::Table> table, int num_partitions, std::vector<std::string> output_paths);
};

#endif //ARROW_SQLITE_ARROW_H


