<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Arrow C++

This directory contains the code and build system for the Arrow C++ libraries,
as well as for the C++ libraries for Apache Parquet.

# Release Build

mkdir parquet_release

cd parquet_release/

cmake -DARROW_PARQUET=ON -DARROW_OPTIONAL_INSTALL=ON -DPARQUET_BUILD_EXAMPLES=ON -DARROW_BUILD_EXAMPLES=ON -DPARQUET_REQUIRE_ENCRYPTION=ON -DARROW_WITH_SNAPPY=ON ..

# Debug Build

mkdir parquet_debug

cd parquet_debug/

cmake -DCMAKE_BUILD_TYPE=Debug -DARROW_PARQUET=ON -DARROW_OPTIONAL_INSTALL=ON -DPARQUET_BUILD_EXAMPLES=ON -DARROW_BUILD_EXAMPLES=ON -DPARQUET_REQUIRE_ENCRYPTION=ON -DARROW_WITH_SNAPPY=ON ..

# Build Binaries

make install

cd parquet_release/examples/parquet

change code, and make all the time

cd arrow/cpp/parquet_release/release

run the app:

./dhl-reader ../../../data

Test data is located at arrow/data folder, there are multiple parquet files.

## possible compilation error

You must have libssl installed to have cryptograpic feature enabled

`sudo apt-get install libssl-dev`

If there are missing so file, you can simply copy them from the installation folder on development machine and store it in the same deployment folder as the executable.

"so" files that you should include are libssl and libcrypt.

## Test on ViX

Copy the binary to vi3-0009 user/release folder

ssh storage008

cd /Volumes/localStorage2/Users/wenlai/release

export LD_LIBRARY_PATH=.

./sqlite-reader M1MOP_W93_FWDHLOSTS_T1_20190813165257

./dhl-reader /mnt/nodes/R6C0S/etlparquet20200323-8

Set proper library path

`export LD_LIBRARY_PATH=.`

Observe search path:

`LD_DEBUG`

## clean the server disk cache

cd /Volumes/localStorage2/bin

on_spark_nodes clean_disk_cache

## Installation

See https://arrow.apache.org/install/ for the latest instructions how
to install pre-compiled binary versions of the library.

## Source Builds and Development

Please refer to our latest [C++ Development Documentation][1].

[1]: https://github.com/apache/arrow/blob/master/docs/source/developers/cpp
