// Stata function: Low-level read full varlist
ST_retcode sf_ll_read_varlist(
    const char *fname,
    const int verbose,
    const int debug,
    const uint64_t strbuffer)
{
    ST_retcode rc = 0;
    int64_t nrow_groups, nrow, ncol, r, i, j;
    int64_t values_read, rows_read, ix, ig;
    clock_t timer = clock();
    SPARQUET_CHAR (vstr, strbuffer);

    // Declare all the value types
    // ---------------------------

    bool vbool;
    int32_t vint32;
    int64_t vint64;
    float vfloat;
    double vdouble;
    parquet::ByteArray vbytearray;

    // Declare all the readers
    // -----------------------

    std::shared_ptr<parquet::ColumnReader> column_reader;
    std::shared_ptr<parquet::RowGroupReader> row_group_reader;

    parquet::BoolReader* bool_reader;
    parquet::Int32Reader* int32_reader;
    parquet::Int64Reader* int64_reader;
    parquet::FloatReader* float_reader;
    parquet::DoubleReader* double_reader;
    parquet::ByteArrayReader* ba_reader;

    // Not implemented in Stata
    // ------------------------

    // parquet::FixedLenByteArray vfixedlen;
    // parquet::Int96 vint96;

    // parquet::Int96Reader* int96_reader;
    // parquet::FixedLenByteArrayReader* flba_reader;

    // File reader
    // -----------

    const parquet::ColumnDescriptor* descr;
    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(fname, false);

        std::shared_ptr<parquet::FileMetaData> file_metadata =
            parquet_reader->metadata();

        ncol = file_metadata->num_columns();
        nrow = file_metadata->num_rows();
        nrow_groups = file_metadata->num_row_groups();
        ix = 0;
        ig = 0;

        sf_printf_debug(verbose, "\tFile:    %s\n",  fname);
        sf_printf_debug(verbose, "\tGroups:  %ld\n", nrow_groups);
        sf_printf_debug(verbose, "\tColumns: %ld\n", ncol);
        sf_printf_debug(verbose, "\tRows:    %ld\n", nrow);

        // Loop through each group
        // For each group, loop through each column
        // For each column, loop through each row

        for (r = 0; r < nrow_groups; ++r) {
            values_read = 0;
            rows_read   = 0;
            ix += ig;
            row_group_reader = parquet_reader->RowGroup(r);
            for (j = 0; j < ncol; j++) {
                i = 0;
                column_reader = row_group_reader->Column(j);
                descr = file_metadata->schema()->Column(j);
                switch (descr->physical_type()) {
                    case Type::BOOLEAN:    // byte
                        bool_reader = static_cast<parquet::BoolReader*>(column_reader.get());
                        while (bool_reader->HasNext()) {
                            rows_read = bool_reader->ReadBatch(1, nullptr, nullptr, &vbool, &values_read);
                            assert(rows_read == 1);
                            assert(values_read == 1);
                            i++;
                            // sf_printf_debug(debug, "\t(bool, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vbool);
                            if ( (rc = SF_vstore(j + 1, i + ix, (ST_double) vbool)) ) goto exit;
                        }
                        break;
                    case Type::INT32:      // long
                        int32_reader = static_cast<parquet::Int32Reader*>(column_reader.get());
                        while (int32_reader->HasNext()) {
                            rows_read = int32_reader->ReadBatch(1, nullptr, nullptr, &vint32, &values_read);
                            assert(rows_read == 1);
                            assert(values_read == 1);
                            i++;
                            // sf_printf_debug(debug, "\t(int32, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vint32);
                            if ( (rc = SF_vstore(j + 1, i + ix, (ST_double) vint32)) ) goto exit;
                        }
                        break;
                    case Type::INT64:      // double
                        int64_reader = static_cast<parquet::Int64Reader*>(column_reader.get());
                        while (int64_reader->HasNext()) {
                            rows_read = int64_reader->ReadBatch(1, nullptr, nullptr, &vint64, &values_read);
                            assert(rows_read == 1);
                            assert(values_read == 1);
                            i++;
                            // sf_printf_debug(debug, "\t(int64, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vint64);
                            if ( (rc = SF_vstore(j + 1, i + ix, (ST_double) vint64)) ) goto exit;
                        }
                        break;
                    case Type::INT96:
                        sf_errprintf("96-bit integers not implemented.\n");
                        rc = 17101;
                        goto exit;
                    case Type::FLOAT:      // float
                        float_reader = static_cast<parquet::FloatReader*>(column_reader.get());
                        while (float_reader->HasNext()) {
                            rows_read = float_reader->ReadBatch(1, nullptr, nullptr, &vfloat, &values_read);
                            assert(rows_read == 1);
                            assert(values_read == 1);
                            i++;
                            // sf_printf_debug(debug, "\t(float, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vfloat);
                            if ( (rc = SF_vstore(j + 1, i + ix, vfloat)) ) goto exit;
                        }
                        break;
                    case Type::DOUBLE:     // double
                        double_reader = static_cast<parquet::DoubleReader*>(column_reader.get());
                        while (double_reader->HasNext()) {
                            rows_read = double_reader->ReadBatch(1, nullptr, nullptr, &vdouble, &values_read);
                            assert(rows_read == 1);
                            assert(values_read == 1);
                            i++;
                            // sf_printf_debug(debug, "\t(double, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vdouble);
                            if ( (rc = SF_vstore(j + 1, i + ix, vdouble)) ) goto exit;
                        }
                        break;
                    case Type::BYTE_ARRAY: // str#, strL
                        ba_reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());
                        while (ba_reader->HasNext()) {
                            rows_read = ba_reader->ReadBatch(1, nullptr, nullptr, &vbytearray, &values_read);
                            assert(rows_read == 1);
                            i++;
                            if ( vbytearray.len > strbuffer ) {
                                sf_errprintf("Buffer (%d) too small; re-run with larger string buffer.\n", strbuffer);
                                sf_errprintf("Group %d, row %d, col %d had a string of length %d.\n", r, i + ix, j, vbytearray.len);
                                rc = 17103;
                                goto exit;
                            }
                            memcpy(vstr, vbytearray.ptr, vbytearray.len);
                            if ( (rc = SF_sstore(j + 1, i + ix, vstr)) ) goto exit;
                            // sf_printf_debug(debug, "\t(BA, %ld, %ld): %s\n", j, i + ix, vstr);
                            memset(vstr, '\0', strbuffer);
                        }
                        break;
                    case Type::FIXED_LEN_BYTE_ARRAY:
                        sf_errprintf("Fixed-Length ByteArray not implemented.\n");
                        rc = 17102;
                        goto exit;
                    default:
                        sf_errprintf("Unknown parquet type.\n");
                        rc = 17100;
                        goto exit;
                }
                ig = i > ig? i: ig;
            }
        }
        sf_running_timer (&timer, "Read data from disk into Stata");

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
    return(rc);
}
