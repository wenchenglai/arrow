// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <iostream>
#include <vector>

#include <arrow/api.h>

using arrow::DoubleBuilder;
using arrow::Int64Builder;
using arrow::ListBuilder;

typedef std::shared_ptr<arrow::Table> table_ptr;

// While we want to use columnar data structures to build efficient operations, we
// often receive data in a row-wise fashion from other systems. In the following,
// we want give a brief introduction into the classes provided by Apache Arrow by
// showing how to transform row-wise data into a columnar table.
//
// The data in this example is stored in the following struct:
struct data_row {
    int64_t id;
    double cost;
    std::vector<double> cost_components;
};

// Transforming a vector of structs into a columnar Table.
//
// The final representation should be an `arrow::Table` which in turn
// is made up of an `arrow::Schema` and a list of
// `arrow::ChunkedArray` instances. As the first step, we will iterate
// over the data and build up the arrays incrementally.  For this
// task, we provide `arrow::ArrayBuilder` classes that help in the
// construction of the final `arrow::Array` instances.
//
// For each type, Arrow has a specially typed builder class. For the primitive
// values `id` and `cost` we can use the respective `arrow::Int64Builder` and
// `arrow::DoubleBuilder`. For the `cost_components` vector, we need to have two
// builders, a top-level `arrow::ListBuilder` that builds the array of offsets and
// a nested `arrow::DoubleBuilder` that constructs the underlying values array that
// is referenced by the offsets in the former array.
arrow::Status VectorToColumnarTable(const std::vector<struct data_row>& rows, std::shared_ptr<arrow::Table>* table, int row_count) {
    // The builders are more efficient using
    // arrow::jemalloc::MemoryPool::default_pool() as this can increase the size of
    // the underlying memory regions in-place. At the moment, arrow::jemalloc is only
    // supported on Unix systems, not Windows.
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    Int64Builder id_builder(pool);
    DoubleBuilder cost_builder(pool);
    ListBuilder components_builder(pool, std::make_shared<DoubleBuilder>(pool));
    // The following builder is owned by components_builder.
    DoubleBuilder& cost_components_builder = *(static_cast<DoubleBuilder*>(components_builder.value_builder()));

    // Now we can loop over our existing data and insert it into the builders. The
    // `Append` calls here may fail (e.g. we cannot allocate enough additional memory).
    // Thus we need to check their return values. For more information on these values,
    // check the documentation about `arrow::Status`.
    for (int i = 0; i < row_count; i++) {
        for (const data_row& row : rows) {
            ARROW_RETURN_NOT_OK(id_builder.Append(row.id));
            ARROW_RETURN_NOT_OK(cost_builder.Append(row.cost));

            // Indicate the start of a new list row. This will memorise the current
            // offset in the values builder.
            ARROW_RETURN_NOT_OK(components_builder.Append());
            // Store the actual values. The final nullptr argument tells the underyling
            // builder that all added values are valid, i.e. non-null.
            ARROW_RETURN_NOT_OK(cost_components_builder.AppendValues(row.cost_components.data(), row.cost_components.size()));
        }
    }

    // At the end, we finalise the arrays, declare the (type) schema and combine them
    // into a single `arrow::Table`:
    std::shared_ptr<arrow::Array> id_array;
    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    std::shared_ptr<arrow::Array> cost_array;
    ARROW_RETURN_NOT_OK(cost_builder.Finish(&cost_array));
    // No need to invoke cost_components_builder.Finish because it is implied by
    // the parent builder's Finish invocation.
    std::shared_ptr<arrow::Array> cost_components_array;
    ARROW_RETURN_NOT_OK(components_builder.Finish(&cost_components_array));

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("id", arrow::int64()),
            arrow::field("cost", arrow::float64()),
            arrow::field("cost_components", arrow::list(arrow::float64()))};

    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    // The final `table` variable is the one we then can pass on to other functions
    // that can consume Apache Arrow memory structures. This object has ownership of
    // all referenced data, thus we don't have to care about undefined references once
    // we leave the scope of the function building the table and its underlying arrays.
    *table = arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});

    return arrow::Status::OK();
}

#define EXIT_ON_FAILURE(expr)                      \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      return EXIT_FAILURE;                         \
    }                                              \
  } while (0);

int main(int argc, char** argv) {
    int table_count = 10;
    int row_count = 50;

    if (argc > 1) {
        table_count = atoi(argv[1]);
    }

    if (argc > 2) {
        row_count = atoi(argv[2]);
    }

    std::vector<data_row> rows = {
            {1, 1.0, {1.0}},
            {2, 2.0, {1.0, 2.0}},
            {3, 3.0, {1.0, 2.0, 3.0}},
            {4, 4.0, {1.0, 2.0, 3.0, 4.0}},
            {5, 5.0, {1.0, 2.0, 3.0, 4.0, 5.0}},
            {6, 6.0, {1.0, 2.0, 3.0, 4.0, 5.0, 6.0}},
            {7, 7.0, {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0}},
            {8, 8.0, {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}},
            {9, 9.0, {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}},
            {10, 10.0, {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}}
    };
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    std::vector<table_ptr> tables;
    for (int i = 0; i < table_count; i++) {
        table_ptr table;
        EXIT_ON_FAILURE(VectorToColumnarTable(rows, &table, row_count / 10));
        std::cout << "Table #" << i + 1 << " loaded rows = " << table->num_rows() << ". Memory alloc:" << pool->bytes_allocated() << ", max: " << pool->max_memory() << std::endl;
        tables.push_back(table);
    }

    arrow::Result<table_ptr> result = arrow::ConcatenateTables(tables);
    table_ptr result_table = result.ValueOrDie();
    std::cout << "After merging " << tables.size() << " tables, row size = " << result_table->num_rows() << std::endl;

    return EXIT_SUCCESS;
}
