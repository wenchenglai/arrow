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

#include "sqlite_util.h"

using arrow::DoubleBuilder;
using arrow::FloatBuilder;
using arrow::Int64Builder;
using arrow::Int32Builder;
using arrow::BinaryBuilder;
using arrow::ListBuilder;

enum memory_pool_type {JePool, SystemPool, MiPool};

#define EXIT_ON_FAILURE(expr)                      \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      return EXIT_FAILURE;                         \
    }                                              \
  } while (0);

#define ABORT_NOT_OK(s)                  \
  do {                                   \
    ::arrow::Status _s = (s);            \
    if (ARROW_PREDICT_FALSE(!_s.ok())) { \
      std::cerr << s.ToString() << "\n"; \
      std::abort();                      \
    }                                    \
  } while (false);

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

arrow::MemoryPool* get_memory_pool(memory_pool_type type) {
    arrow::MemoryPool* pool;
    if (type == SystemPool) {
        pool = arrow::system_memory_pool();
    } else if (type == MiPool) {
        ABORT_NOT_OK(arrow::mimalloc_memory_pool(&pool));
    } else {
        pool = arrow::default_memory_pool();
    }
    return pool;
}

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

template<typename T>
arrow::Status print_builder_summary(T builder) {
    int length = builder->length();
    string type = builder->type()->ToString();

    std::cout << "Type: " << type << ", cap: " << builder->capacity() << ", length: " << length
    << ", children: " << builder->num_children() << std::endl;

    for (int i = 0; i < length; i++) {
        std::cout << i << " value = " << builder->GetValue(1) << std::endl;
    }

    return arrow::Status::OK();
}

arrow::Status print_binary_builder_summary(std::shared_ptr<BinaryBuilder> builder) {
    int length = builder->length();
    string type = builder->type()->ToString();

    std::cout << "Type: " << type << ", cap: " << builder->capacity() << ", length: " << length
              << ", children: " << builder->num_children() << std::endl;

    for (int i = 0; i < length; i++) {
        arrow::util::string_view view = builder->GetView(i);

        int j = 0;
        const uint8_t* bbb = builder->GetValue(i, &j);

        std::cout << i << std::endl;
        std::cout << "value_data: value = " << builder->value_data() << ", capacity = " << builder->value_data_capacity() << std::endl;
        std::cout << "GetValue: value = " << *bbb << ", and j = " << j << std::endl;
        std::cout << "string view: value = " << view << std::endl;
        std::cout << "string view[0]: value = " << view[0] << std::endl;
        std::cout << "string view[1]: value = " << view[1] << std::endl;
        std::cout << "string view[2]: value = " << view[2] << std::endl;

        std::cout << "**************" << std::endl;
    }

    return arrow::Status::OK();
}

arrow::Status dynamic_columns_load(table_ptr* table, int row_count, string_map const &source_schema_map, memory_pool_type pool_type) {
    arrow::MemoryPool* pool = get_memory_pool(pool_type);

    std::unordered_map<string, std::shared_ptr<DoubleBuilder>> double_builder_map;
    std::unordered_map<string, std::shared_ptr<FloatBuilder>> float_builder_map;
    std::unordered_map<string, std::shared_ptr<Int64Builder>> int64_builder_map;
    std::unordered_map<string, std::shared_ptr<Int32Builder>> int32_builder_map;
    std::unordered_map<string, std::shared_ptr<BinaryBuilder>> binary_builder_map;

    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        string col_name = itr->first;
        string col_type = itr->second;

        if ("DOUBLE" == col_type) {
            double_builder_map[col_name] = std::make_shared<DoubleBuilder>(arrow::float64(), pool);

        } else if ("FLOAT" == col_type) {
            float_builder_map[col_name] = std::make_shared<FloatBuilder>(arrow::float32(), pool);

        } else if ("BIGINT" == col_type) {
            int64_builder_map[col_name] = std::make_shared<Int64Builder>(arrow::int64(), pool);

        } else if ("INTEGER" == col_type) {
            int32_builder_map[col_name] = std::make_shared<Int32Builder>(arrow::int32(), pool);

        } else if ("BLOB" == col_type) {
            binary_builder_map[col_name] = std::make_shared<BinaryBuilder>(arrow::binary(), pool);
        }
    }

//    std::cout << "Size of double_builder_map = " << double_builder_map.size() << std::endl;
//    std::cout << "Size of float_builder_map = " << float_builder_map.size() << std::endl;
//    std::cout << "Size of int64_builder_map = " << int64_builder_map.size() << std::endl;
//    std::cout << "Size of int32_builder_map = " << int32_builder_map.size() << std::endl;
//    std::cout << "Size of binary_builder_map = " << binary_builder_map.size() << std::endl;

//    BinaryBuilder bu;
//
//
//
//    std::shared_ptr<BinaryBuilder> builder = binary_builder_map["iADCVector"];
//    ARROW_RETURN_NOT_OK(builder->Reserve(1000));
//    print_builder_summary(builder);
//
//    int blob_size = 2;
//    uint8_t local_buffer[blob_size];
//    local_buffer[0] = 65;
//    local_buffer[1] = 66;
//    ARROW_RETURN_NOT_OK(builder->Append(local_buffer, blob_size));
//    print_builder_summary(builder);
//
//    int blob_size2 = 4;
//    uint8_t local_buffer2[blob_size2];
//    local_buffer2[0] = 67;
//    local_buffer2[1] = 68;
//    local_buffer2[2] = 69;
//    local_buffer2[3] = 70;
//    ARROW_RETURN_NOT_OK(builder->Append(local_buffer2, blob_size2));
//    print_builder_summary(builder);

    for (int i = 0; i < row_count; i++) {
        for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
            string col_name = itr->first;
            string col_type = itr->second;

            if ("DOUBLE" == col_type) {
                std::shared_ptr<DoubleBuilder> builder = double_builder_map[col_name];
                ARROW_RETURN_NOT_OK(builder->Append(64));

            } else if ("FLOAT" == col_type) {
                std::shared_ptr<FloatBuilder> builder = float_builder_map[col_name];
                ARROW_RETURN_NOT_OK(builder->Append(64));

            } else if ("BIGINT" == col_type) {
                std::shared_ptr<Int64Builder> builder = int64_builder_map[col_name];
                ARROW_RETURN_NOT_OK(builder->Append(64));

            } else if ("INTEGER" == col_type ) {
                std::shared_ptr<Int32Builder> builder = int32_builder_map[col_name];
                ARROW_RETURN_NOT_OK(builder->Append(64));

            } else if ("BLOB" == col_type) {
                int blob_size = 1;
                uint8_t local_buffer[blob_size];
                local_buffer[0] = 64;
                std::shared_ptr<BinaryBuilder> builder = binary_builder_map[col_name];
                ARROW_RETURN_NOT_OK(builder->Append(local_buffer, blob_size));
            }
        }
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    for (auto itr = source_schema_map.begin(); itr != source_schema_map.end(); itr++) {
        string col_name = itr->first;
        string col_type = itr->second;

        std::shared_ptr<arrow::Array> array;
        if ("DOUBLE" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::float64()));
            std::shared_ptr<DoubleBuilder> builder = double_builder_map[col_name];
            ARROW_RETURN_NOT_OK(builder->Finish(&array));

        } else if ("FLOAT" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::float32()));
            std::shared_ptr<FloatBuilder> builder = float_builder_map[col_name];
            ARROW_RETURN_NOT_OK(builder->Finish(&array));

        } else if ("BIGINT" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::int64()));
            std::shared_ptr<Int64Builder> builder = int64_builder_map[col_name];
            ARROW_RETURN_NOT_OK(builder->Finish(&array));

        } else if ("INTEGER" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::int32()));
            std::shared_ptr<Int32Builder> builder = int32_builder_map[col_name];
            ARROW_RETURN_NOT_OK(builder->Finish(&array));

        } else if ("BLOB" == col_type) {
            schema_vector.emplace_back(arrow::field(col_name, arrow::binary()));
            std::shared_ptr<BinaryBuilder> builder = binary_builder_map[col_name];
            ARROW_RETURN_NOT_OK(builder->Finish(&array));
        }

        arrays.emplace_back(array);
    }

    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    *table = arrow::Table::Make(schema, arrays);

    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    int table_count = 10;
    int row_count = 50;
    memory_pool_type pool_type = JePool;

    if (argc > 1) {
        table_count = atoi(argv[1]);
    }

    if (argc > 2) {
        row_count = atoi(argv[2]);
    }

    if (argc > 3) {
        string pool_name = argv[3];

        if ("system" == pool_name) {
            pool_type = SystemPool;
        } else if ("mi" == pool_name) {
            pool_type = MiPool;
        }
    }

    std::cout << "We are creating " << table_count << " tables, each has " << row_count << " rows, using memory pool: " << pool_type << std::endl;

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

    // create data source db schema, as it's needed for arrow table creation
    // it's better to get schema here, because every thread need the same schema object
    string sqlite_file = "channel0.patch";
    string_map source_schema_map;
    get_schema(sqlite_file, source_schema_map);
    //print_dhl_sqlite_schema(source_schema_map);

    auto start = std::chrono::steady_clock::now();

    arrow::MemoryPool* pool = get_memory_pool(pool_type);

    std::vector<table_ptr> tables;
    for (int i = 0; i < table_count; i++) {
        table_ptr table;

        //EXIT_ON_FAILURE(VectorToColumnarTable(rows, &table, row_count / 10));

        EXIT_ON_FAILURE(dynamic_columns_load(&table, row_count, source_schema_map, pool_type));

        std::cout << "Table #" << i + 1 << " loaded rows = " << table->num_rows() << ". Memory alloc:" << pool->bytes_allocated() << ", max: " << pool->max_memory() << std::endl;
        tables.push_back(table);
    }

    arrow::Result<table_ptr> result = arrow::ConcatenateTables(tables);
    table_ptr result_table = result.ValueOrDie();
    std::cout << "After merging " << tables.size() << " tables, row size = " << result_table->num_rows() << std::endl;

    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "Total elapsed time: " << elapsed_seconds.count() << " seconds. " << std::endl;

    return EXIT_SUCCESS;
}
