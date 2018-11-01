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

// high-level

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

// low-level

#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

using parquet::LogicalType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

Type::type get_physical_type(const arrow::Type::type type) {
    switch (type) {
        case arrow::Type::BOOL:
            return Type::BOOLEAN;
        case arrow::Type::UINT8:
        case arrow::Type::INT8:
        case arrow::Type::UINT16:
        case arrow::Type::INT16:
        case arrow::Type::UINT32:
        case arrow::Type::INT32:
            return Type::INT32;
        case arrow::Type::UINT64:
        case arrow::Type::INT64:
            return Type::INT64;
        case arrow::Type::FLOAT:
            return Type::FLOAT;
        case arrow::Type::DOUBLE:
            return Type::DOUBLE;
        case arrow::Type::BINARY:
            return Type::BYTE_ARRAY;
        case arrow::Type::STRING:
            return Type::BYTE_ARRAY;
        case arrow::Type::FIXED_SIZE_BINARY:
        case arrow::Type::DECIMAL:
            return Type::FIXED_LEN_BYTE_ARRAY;
        case arrow::Type::DATE32:
            return Type::INT32;
        case arrow::Type::DATE64:
            // Convert to date32 internally
            return Type::INT32;
        case arrow::Type::TIME32:
            return Type::INT32;
        case arrow::Type::TIME64:
            return Type::INT64;
        case arrow::Type::TIMESTAMP:
            return Type::INT64;
        case arrow::Type::DICTIONARY:
            // not implemented; figure a better way to deal?
            return Type::INT96;
        default:
            break;
    }
    return Type::INT32;
}
