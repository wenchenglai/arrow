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

A few DHL data performance software runners are created: sqlite-to-parquet, sqlite-to-arrow, dhl-reader

To compile performance tester application or the examples, you can choose to either build a debug or release version.

Please go to /arrow/cpp folder and follow the steps below:

# Run Tests

`ctest j16 --output-on-failure`

# Development Guidelines

Please read the Google C++ Styles Guidelines

https://google.github.io/styleguide/cppguide.html

## Release Build

`mkdir parquet_release`

`cd parquet_release/`

`cmake -DARROW_PARQUET=ON -DARROW_OPTIONAL_INSTALL=ON -DARROW_MIMALLOC=ON -DPARQUET_BUILD_EXAMPLES=ON -DARROW_BUILD_EXAMPLES=ON -DPARQUET_REQUIRE_ENCRYPTION=ON -DARROW_WITH_SNAPPY=ON ..`

## Debug Build

`mkdir parquet_debug`

`cd parquet_debug/`

`cmake -DCMAKE_BUILD_TYPE=Debug -DARROW_MIMALLOC=ON -DARROW_PARQUET=ON -DARROW_OPTIONAL_INSTALL=ON -DPARQUET_BUILD_EXAMPLES=ON -DARROW_BUILD_EXAMPLES=ON -DPARQUET_REQUIRE_ENCRYPTION=ON -DARROW_WITH_SNAPPY=ON ..`

## Build Binaries

Next, let's start building the artifacts:

`make install`

Go to your folder (debug/release):
`cd parquet_release/examples/parquet`

then run:

`make`

if you want to have arrow examples, go to arrow examples folder:

`cd parquet_release/examples/arrow`

then run:

`make`

Every change you make, you need to run the `make` command in this folder.

The artifacts are published in here (debug/release):

`cd arrow/cpp/parquet_release/release`

run the app, e.g.:

Run an simple parquet encryption example:
`./parquet-encryption-example`

Run DHL Parquet data loader into Arrow table:
`./dhl-reader parquet-folder/ 1`

Run DHL SQLite data loader into Arrow table:
`./sqlite-to-arrow dhl-name`

## Testing Data

Test data is located at arrow/data folder, there are multiple parquet files.

### possible compilation error

You must have libssl installed to have cryptograpic feature enabled

`sudo apt-get install libssl-dev`

If there are missing so file, you can simply copy them from the installation folder on development machine and store it in the same deployment folder as the executable.

"so" files that you should include are libssl and libcrypt.

## Deploy to VIVA

First, create symbolic links to necessary libraries.

libarrow.so.100.0.0 is the root, real library.  Create two symbolic links as below:

`ln -s libarrow.so.100.0.0 libarrow.so.100`
`ln -s libarrow.so.100 libarrow.so`

libparquet.so.100.0.0 is the root, real library.  Create two symbolic links as below:

`ln -s libparquet.so.100.0.0 libparquet.so.100`
`ln -s libparquet.so.100 libparquet.so`

## Test on ViXX

Copy the binaries to vi3-0009 your-own-folder/release folder, I use my own folder as example:

`ssh storage008`

`cd /Volumes/localStorage2/Users/wenlai/release`

`export LD_LIBRARY_PATH=.`

`./sqlite-reader M1MOP_W93_FWDHLOSTS_T1_20190813165257`

`./dhl-reader /mnt/nodes/R6C0S/etlparquet20200323-8`

## Debugging tips
Set proper library path if necessary.  By default, use the local folder:

`export LD_LIBRARY_PATH=.`

Observe search path:

`LD_DEBUG`

Check library dependencies:

`ldd -v your-so-file`

## clean the server disk cache

You need to clear all the disk cache, else your performance result will be skewed:

`cd /Volumes/localStorage2/bin`

`on_spark_nodes clean_disk_cache`

