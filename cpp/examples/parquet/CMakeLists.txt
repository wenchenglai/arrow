# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

add_executable(parquet-low-level-example low-level-api/reader-writer.cc)
add_executable(parquet-low-level-example2 low-level-api/reader-writer2.cc)
add_executable(parquet-arrow-example parquet-arrow/reader-writer.cc)
add_executable(parquet-stream-api-example parquet-stream-api/stream-reader-writer.cc)
add_executable(dhl-reader low-level-api/dhl-reader.cc low-level-api/sqlite3.c low-level-api/sqlite_arrow.cpp low-level-api/sqlite3_util.cpp)
add_executable(sqlite-reader low-level-api/sqlite-reader.cc low-level-api/sqlite3.c)
add_executable(sqlite-to-parquet low-level-api/sqlite-to-parquet.cc low-level-api/sqlite3.c)
add_executable(sqlite-arrow-tester low-level-api/sqlite-arrow-tester.cpp low-level-api/sqlite3.c low-level-api/sqlite_arrow.cpp low-level-api/sqlite3_util.cpp)
add_executable(sqlite-to-arrow low-level-api/sqlite-to-arrow.cc low-level-api/sqlite3.c)
add_executable(arrow-tester low-level-api/arrow_tester.cc low-level-api/sqlite3.c low-level-api/sqlite3_util.cpp)
add_executable(sqlite-to-dhlx low-level-api/sqlite-to-dhlx.cpp low-level-api/sqlite3.c low-level-api/sqlite_arrow.cpp low-level-api/sqlite3_util.cpp)
add_executable(sqlite-to-sqlite
        low-level-api/sqlite-to-sqlite.cpp
        low-level-api/library/sqlite3.c
        low-level-api/library/sqlite_arrow.cpp
        low-level-api/library/sqlite3_util.cpp
        low-level-api/library/locator_key.cpp
        low-level-api/library/dhl_fs.cpp)

add_executable(locator_key_test
        low-level-api/test/locator_key_test.cpp
        low-level-api/library/locator_key.cpp
        low-level-api/library/dhl_fs.cpp)



target_include_directories(parquet-low-level-example PRIVATE low-level-api/)
target_include_directories(parquet-low-level-example2 PRIVATE low-level-api/)

if (PARQUET_REQUIRE_ENCRYPTION)
  add_executable(parquet-encryption-example low-level-api/encryption-reader-writer.cc)
  add_executable(parquet-encryption-example-all-crypto-options low-level-api/encryption-reader-writer-all-crypto-options.cc)
  target_include_directories(parquet-encryption-example PRIVATE low-level-api/)
  target_include_directories(parquet-encryption-example-all-crypto-options PRIVATE low-level-api/)
endif()

# Prefer shared linkage but use static if shared build is deactivated
if (ARROW_BUILD_SHARED)
  set(PARQUET_EXAMPLE_LINK_LIBS parquet_shared)
else()
  set(PARQUET_EXAMPLE_LINK_LIBS parquet_static)
endif()

target_link_libraries(parquet-arrow-example ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(parquet-low-level-example ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(parquet-low-level-example2 ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(parquet-stream-api-example ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(dhl-reader ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(sqlite-reader ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(sqlite-to-parquet ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(sqlite-arrow-tester ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(sqlite-to-arrow ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(arrow-tester ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(sqlite-to-dhlx ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(sqlite-to-sqlite ${PARQUET_EXAMPLE_LINK_LIBS})

if(PARQUET_REQUIRE_ENCRYPTION)
  target_link_libraries(parquet-encryption-example ${PARQUET_EXAMPLE_LINK_LIBS})
  target_link_libraries(parquet-encryption-example-all-crypto-options ${PARQUET_EXAMPLE_LINK_LIBS})
endif()

add_dependencies(parquet
  parquet-low-level-example
  parquet-low-level-example2
  parquet-arrow-example
  parquet-stream-api-example)

if (PARQUET_REQUIRE_ENCRYPTION)
  add_dependencies(parquet
    parquet-encryption-example
    parquet-encryption-example-all-crypto-options)
endif()
