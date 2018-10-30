// try {
//
//     // Create a ParquetReader instance; get file MetaData
//     // --------------------------------------------------
//
//     std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
//         parquet::ParquetFileReader::OpenFile(PARQUET_FILENAME, false);
//
//     const parquet::FileMetaData* file_metadata = parquet_reader->metadata().get();
//     // std::shared_ptr<parquet::FileMetaData> file_metadata =
//     //     parquet_reader->metadata();
//
//     sf_printf_debug("Real Columns:  %ld\n", file_metadata->schema()->group_node()->field_count());
//     sf_printf_debug("Total Columns: %ld\n", file_metadata->num_columns());
//     sf_printf_debug("Total rows:    %ld\n", file_metadata->num_rows());
//
//     // Get the number of RowGroups, Columns
//     // ------------------------------------
//
//     int64_t nrow_groups = file_metadata->num_row_groups();
//     int64_t ncol = file_metadata->num_columns();
//
//     // Declare all the readers
//     std::shared_ptr<parquet::ColumnReader> column_reader;
//     std::shared_ptr<parquet::RowGroupReader> row_group_reader;
//     parquet::BoolReader* bool_reader;
//     parquet::Int32Reader* int32_reader;
//     parquet::Int64Reader* int64_reader;
//     parquet::FloatReader* float_reader;
//     parquet::DoubleReader* double_reader;
//     parquet::Int96Reader* int96_reader;
//     parquet::ByteArrayReader* ba_reader;
//     parquet::FixedLenByteArrayReader* flba_reader;
//
//     // Iterate over all the RowGroups in the file
//     for (r = 0; r < nrow_groups; ++r) {
//         values_read = 0;
//         rows_read   = 0;
//
//         // Get the RowGroup Reader
//         row_group_reader = parquet_reader->RowGroup(r);
//
//         for (j = 0; j < ncol; j++) {
//             column_reader = row_group_reader->Column(j);
//             parquet::ColumnReader* switch_reader =
//                 static_cast<parquet::ColumnReader*>(column_reader.get());
//
//             const parquet::ColumnDescriptor* descr =
//                 file_metadata->schema()->Column(j);
//
//             i = 0;
//             sf_printf_debug("\nColumn %ld: %s (%s)\n", j, descr->name().c_str(), TypeToString(descr->physical_type()).c_str());
//             switch (switch_reader->type()) {
//                 case Type::BOOLEAN:
//                     bool_reader = static_cast<parquet::BoolReader*>(column_reader.get());
//                     while (bool_reader->HasNext()) {
//                         rows_read = bool_reader->ReadBatch(1, nullptr, nullptr, &vbool, &values_read);
//                         assert(rows_read == 1);
//                         assert(values_read == 1);
//                         i++;
//                         sf_printf_debug("\t%ld: %d\n", i, vbool);
//                         if ( (rc = SF_vstore(j + 1, i, (ST_double) vbool)) ) goto exit;
//                     }
//                 case Type::INT32:
//                     int32_reader = static_cast<parquet::Int32Reader*>(column_reader.get());
//                     while (int32_reader->HasNext()) {
//                         rows_read = int32_reader->ReadBatch(1, nullptr, nullptr, &vint32, &values_read);
//                         assert(rows_read == 1);
//                         assert(values_read == 1);
//                         i++;
//                         sf_printf_debug("\t%ld: %d\n", i, vint32);
//                         if ( (rc = SF_vstore(j + 1, i, vint32)) ) goto exit;
//                     }
//                 case Type::INT64:
//                     int64_reader = static_cast<parquet::Int64Reader*>(column_reader.get());
//                     while (int64_reader->HasNext()) {
//                         rows_read = int64_reader->ReadBatch(1, nullptr, nullptr, &vint64, &values_read);
//                         assert(rows_read == 1);
//                         assert(values_read == 1);
//                         i++;
//                         sf_printf_debug("\t%ld: %ld\n", i, vint64);
//                         if ( (rc = SF_vstore(j + 1, i, (ST_double) vint64)) ) goto exit;
//                     }
//                 case Type::INT96:
//                     int96_reader = static_cast<parquet::Int96Reader*>(column_reader.get());
//                     while (int96_reader->HasNext()) {
//                         rows_read = int96_reader->ReadBatch(1, nullptr, nullptr, &vint96, &values_read);
//                         assert(rows_read == 1);
//                         assert(values_read == 1);
//                         i++;
//                         sf_printf_debug("\t%ld: (96)\n", i);
//                     }
//                 case Type::FLOAT:
//                     float_reader = static_cast<parquet::FloatReader*>(column_reader.get());
//                     while (float_reader->HasNext()) {
//                         rows_read = float_reader->ReadBatch(1, nullptr, nullptr, &vfloat, &values_read);
//                         assert(rows_read == 1);
//                         assert(values_read == 1);
//                         i++;
//                         sf_printf_debug("\t%ld: %9.4f\n", i, vfloat);
//                         if ( (rc = SF_vstore(j + 1, i, vfloat)) ) goto exit;
//                     }
//                 case Type::DOUBLE:
//                     double_reader = static_cast<parquet::DoubleReader*>(column_reader.get());
//                     while (double_reader->HasNext()) {
//                         rows_read = double_reader->ReadBatch(1, nullptr, nullptr, &vdouble, &values_read);
//                         assert(rows_read == 1);
//                         assert(values_read == 1);
//                         i++;
//                         sf_printf_debug("\t%ld: %9.4f\n", i, vdouble);
//                         if ( (rc = SF_vstore(j + 1, i, vdouble)) ) goto exit;
//                     }
//                 case Type::BYTE_ARRAY:
//                     ba_reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());
//                     while (ba_reader->HasNext()) {
//                         rows_read = ba_reader->ReadBatch(1, nullptr, nullptr, &vbytearray, &values_read);
//                         assert(rows_read == 1);
//                         i++;
//                         sf_printf_debug("\t%ld: (ba: %d, %s)\n", i, vbytearray.len, vbytearray.ptr);
//                         memcpy(vstr, vbytearray.ptr, vbytearray.len);
//                         if ( (rc = SF_sstore(j + 1, i, vstr)) ) goto exit;
//                         memset(vstr, '\0', maxstrlen);
//                         // memcmp(value.ptr, &expected_value[0], FIXED_LENGTH) == 0
//                     }
//                 case Type::FIXED_LEN_BYTE_ARRAY:
//                     flba_reader = static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());
//                     while (flba_reader->HasNext()) {
//                         rows_read = flba_reader->ReadBatch(1, nullptr, nullptr, &vfixedlen, &values_read);
//                         assert(rows_read == 1);
//                         assert(values_read == 1);
//                         i++;
//                         sf_printf_debug("\t%ld: (flba: %d, %s)\n", i, descr->type_length(), vfixedlen.ptr);
//                     }
//                 default:
//                     sf_printf_debug("hi WHY??\n");
//             }
//         }
//     }
//
// } catch (const std::exception& e) {
//     std::cerr << "Parquet read error: " << e.what() << std::endl;
//     return -1;
// }
