#ifndef ARROW_SQLITE_ARROW_H
#define ARROW_SQLITE_ARROW_H

#include "sqlite3_util.h"
#include "locator_key.h"

void tokenize(std::string const &str, const char delim, std::vector<std::string> &out);

class SqliteArrow {
public:
    /// \brief Convert SQLite data in disk to Arrow Table in memory
    /// \param[string] dhl_name the name of dhl
    /// \param[string] input_path the root input path, the next level must be RXC01 folders
    /// \param[std::shared_ptr<arrow::Table>*] out_table an Arrow table pointer to the result
    /// \return status indicate EXIT_SUCCESS or EXIT_FAILURE
    std::shared_ptr<arrow::Table> SQLiteToArrow(
            std::string dhl_name,
            std::string input_path,
            std::vector<std::uint64_t> selector_locator_ids);

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

    /// \brief Convert Arrow RecordBatch in memory to SQLite file with specified location
    /// \param[std::shared_ptr<arrow::RecordBatch>] record_batch Arrow RecordBatch that contains data for a single
    /// SQLite table.  This table must have location aware ID.
    ///
    /// \param[std::string] dhl_name the name of the DHL
    ///
    /// \param[std::string] output_file_path root path that contains RXC0S folders
    ///
    /// \return status indicate EXIT_SUCCESS or EXIT_FAILURE
    int record_batch_to_sqlite(std::shared_ptr<arrow::RecordBatch> record_batch, std::string dhl_name, std::string output_file_path);

};

#endif //ARROW_SQLITE_ARROW_H


