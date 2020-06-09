#ifndef ARROW_SQLITE_ARROW_H
#define ARROW_SQLITE_ARROW_H

#include "sqlite3_util.h"

void tokenize(std::string const &str, const char delim, std::vector<std::string> &out);

class SqliteArrow {
public:
    /// \brief Convert SQLite data in disk to Arrow Table in memory
    /// \param[string] dhl_name the name of dhl
    /// \param[string] input_path the root input path, the next level must be RXC01
    /// \param[std::shared_ptr<arrow::Table>*] out_table an Arrow table pointer that will point to the
    /// newly generated Arrow table
    /// \return status indicate EXIT_SUCCESS or EXIT_FAILURE
    int sqlite_to_arrow(std::string dhl_name, std::string input_path, std::shared_ptr<arrow::Table>* out_table);

    /// \brief Convert Arrow Table in memory to SQLite data in disk
    /// \param[std::shared_ptr<arrow::Table>] table the length of the slice. If there are not enough
    /// elements in the array, the length will be adjusted accordingly
    ///
    /// \param[std::string] output_file_path the length of the slice. If there are not enough
    /// elements in the array, the length will be adjusted accordingly
    ///
    /// \return status indicate EXIT_SUCCESS or EXIT_FAILURE
    int arrow_to_sqlite(std::shared_ptr<arrow::Table> table, std::string output_file_path);

    /// \brief Split Arrow table into x partitions and save those into output_paths
    /// \param[std::shared_ptr<arrow::Table>] table input Arrow table
    /// \param[int] num_partitions number of partitions to divide the input table.
    /// This input parameter will not be used of output_paths is specified
    ///
    /// \param[std::vector<std::string>] output_paths a list of output path strings.  If provided,
    /// it will override num_partitions. The output_paths size will determine the partition count.
    ///
    /// \return status indicate EXIT_SUCCESS or EXIT_FAILURE
    int arrow_to_sqlite_split(std::shared_ptr<arrow::Table> table, int num_partitions, std::vector<std::string> output_paths);
};

#endif //ARROW_SQLITE_ARROW_H


