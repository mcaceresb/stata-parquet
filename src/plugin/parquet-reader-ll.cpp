// Stata function: Low-level read full varlist
// __sparquet_infrom must exist and be a numeric scalar
// __sparquet_into must exist and be a numeric scalar
ST_retcode sf_ll_read_varlist(
    const char *fname,
    const int verbose,
    const int debug,
    const uint64_t strbuffer)
{
    ST_double z;
    ST_retcode rc = 0, any_rc = 0;

    bool is_null;
    int64_t maxstrlen, ix, ig, infrom, into;
    int64_t nrow_groups, ncol, r, i, j, jsel;
    int64_t warn_strings = 0;

    SPARQUET_CHAR (vmatrix, 32);
    SPARQUET_CHAR (vscalar, 32);

    // Declare all the value types
    // ---------------------------

    bool vbool;
    int32_t vint32;
    int64_t vint64;
    float vfloat;
    double vdouble;
    parquet::ByteArray vbytearray;
    parquet::FixedLenByteArray vfixedlen;

    // Declare all the readers
    // -----------------------

    // std::shared_ptr<parquet::ColumnReader> column_reader;
    std::shared_ptr<parquet::RowGroupReader> row_group_reader;

    std::shared_ptr<parquet::BoolScanner>  bool_scanner;
    std::shared_ptr<parquet::Int32Scanner> int32_scanner;
    std::shared_ptr<parquet::Int64Scanner> int64_scanner;
    std::shared_ptr<parquet::FloatScanner> float_scanner;
    std::shared_ptr<parquet::DoubleScanner> double_scanner;
    std::shared_ptr<parquet::ByteArrayScanner> ba_scanner;
    std::shared_ptr<parquet::FixedLenByteArrayScanner> flba_scanner;

    // Not implemented in Stata
    // ------------------------

    // parquet::Int96 vint96;
    // parquet::Int96Reader* int96_reader;

    // File reader
    // -----------

    const parquet::ColumnDescriptor* descr;
    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(fname, false);

        std::shared_ptr<parquet::FileMetaData> file_metadata =
            parquet_reader->metadata();

        // ncol = file_metadata->num_columns();
        nrow_groups = file_metadata->num_row_groups();
        ix = 0;
        ig = 0;

        memset(vscalar, '\0', 32);
        memcpy(vscalar, "__sparquet_infrom", 17);
        if ( (rc = SF_scal_use(vscalar, &z)) ) {
            any_rc = rc;
            infrom = 1;
        }
        else {
            infrom = (int64_t) z - 1;
        }

        memset(vscalar, '\0', 32);
        memcpy(vscalar, "__sparquet_into", 15);
        if ( (rc = SF_scal_use(vscalar, &z)) ) {
            any_rc = rc;
            into = 1;
        }
        else {
            into = (int64_t) z - 1;
        }

        memset(vscalar, '\0', 32);
        memcpy(vscalar, "__sparquet_ncol", 15);
        if ( (rc = SF_scal_use(vscalar, &z)) ) {
            any_rc = rc;
            ncol = 1;
        }
        else {
            ncol = (int64_t) z;
        }

        maxstrlen = 1;
        int64_t vtypes[ncol];
        int64_t colix[ncol];

        // Adjust column selection to be 0-indexed
        memset(vmatrix, '\0', 32);
        memcpy(vmatrix, "__sparquet_colix", 16);
        for (j = 0; j < ncol; j++) {
            if ( (rc = SF_mat_el(vmatrix, 1, j + 1, &z)) ) goto exit;
            colix[j] = (int64_t) z - 1;
        }

        // Encoded variable types
        memset(vmatrix, '\0', 32);
        memcpy(vmatrix, "__sparquet_coltypes", 19);
        for (j = 0; j < ncol; j++) {
            if ( (rc = SF_mat_el(vmatrix, 1, j + 1, &z)) ) goto exit;
            vtypes[j] = (int64_t) z;
            if ( vtypes[j] > maxstrlen ) maxstrlen = vtypes[j];
        }
        SPARQUET_CHAR (vstr, maxstrlen);

        if ( any_rc ) {
            rc = any_rc;
            goto exit;
        }

        sf_printf_debug(verbose, "\tFile:    %s\n",  fname);
        sf_printf_debug(verbose, "\tGroups:  %ld\n", nrow_groups);
        sf_printf_debug(verbose, "\tColumns: %ld\n", ncol);
        sf_printf_debug(verbose, "\tRows:    %ld\n", into - infrom + 1);

        // Loop through each group
        // For each group, loop through each column
        // For each column, loop through each row

        clock_t timer = clock();
        for (r = 0; r < nrow_groups; ++r) {
            ix += ig;
            if ( ix > into ) break;
            row_group_reader = parquet_reader->RowGroup(r);
            for (j = 0; j < ncol; j++) {
                i = 0;
                jsel = colix[j];
                // column_reader = row_group_reader->Column(jsel);
                descr = file_metadata->schema()->Column(jsel);
                switch (descr->physical_type()) {
                    case Type::BOOLEAN:    // byte
                        bool_scanner = std::make_shared<parquet::BoolScanner>(row_group_reader->Column(jsel));
                        while (bool_scanner->HasNext()) {
                            bool_scanner->NextValue(&vbool, &is_null);
                            if ( i++ < (infrom - ix) ) continue;
                            // sf_printf_debug(2, "\t(bool, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vbool);
                            if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: (ST_double) vbool)) ) goto exit;
                            if ( i > (into - ix) ) break;
                        }
                        break;
                    case Type::INT32:      // long
                        int32_scanner = std::make_shared<parquet::Int32Scanner>(row_group_reader->Column(jsel));
                        while (int32_scanner->HasNext()) {
                            int32_scanner->NextValue(&vint32, &is_null);
                            if ( i++ < (infrom - ix) ) continue;
                            // sf_printf_debug(2, "\t(int32, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vint32);
                            if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: (ST_double) vint32)) ) goto exit;
                            if ( i > (into - ix) ) break;
                        }
                        break;
                    case Type::INT64:      // double
                        int64_scanner = std::make_shared<parquet::Int64Scanner>(row_group_reader->Column(jsel));
                        while (int64_scanner->HasNext()) {
                            int64_scanner->NextValue(&vint64, &is_null);
                            if ( i++ < (infrom - ix) ) continue;
                            // sf_printf_debug(2, "\t(int64, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vint64);
                            if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: (ST_double) vint64)) ) goto exit;
                            if ( i > (into - ix) ) break;
                        }
                        break;
                    case Type::INT96:
                        sf_errprintf("96-bit integers not implemented.\n");
                        rc = 17101;
                        goto exit;
                    case Type::FLOAT:      // float
                        float_scanner = std::make_shared<parquet::FloatScanner>(row_group_reader->Column(jsel));
                        while (float_scanner->HasNext()) {
                            float_scanner->NextValue(&vfloat, &is_null);
                            if ( i++ < (infrom - ix) ) continue;
                            // sf_printf_debug(2, "\t(float, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vfloat);
                            if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: (ST_double) vfloat)) ) goto exit;
                            if ( i > (into - ix) ) break;
                        }
                        break;
                    case Type::DOUBLE:     // double
                        double_scanner = std::make_shared<parquet::DoubleScanner>(row_group_reader->Column(jsel));
                        while (double_scanner->HasNext()) {
                            double_scanner->NextValue(&vdouble, &is_null);
                            if ( i++ < (infrom - ix) ) continue;
                            // sf_printf_debug(debug, "\t(double, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vdouble);
                            if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: vdouble)) ) goto exit;
                            if ( i > (into - ix) ) break;
                        }
                        break;
                    case Type::BYTE_ARRAY: // str#, strL
                        ba_scanner = std::make_shared<parquet::ByteArrayScanner>(row_group_reader->Column(jsel));
                        while (ba_scanner->HasNext()) {
                            ba_scanner->NextValue(&vbytearray, &is_null);
                            if ( i++ < (infrom - ix) ) continue;
                            if ( is_null ) {
                                warn_strings++;
                            }
                            else if ( vbytearray.len > vtypes[j] ) {
                                sf_errprintf("Buffer (%d) too small; re-run with larger buffer or -strscan(.)-\n", vtypes[j]);
                                sf_errprintf("Group %d, row %d, col %d had a string of length %d.\n", r, i + ix, j, vbytearray.len);
                                rc = 17103;
                                goto exit;
                            }
                            else {
                                memcpy(vstr, vbytearray.ptr, vbytearray.len);
                                // sf_printf_debug(debug, "\t(BA, %ld, %ld): %s\n", j, i + ix, vstr);
                                if ( (rc = SF_sstore(j + 1, i + ix - infrom, vstr)) ) goto exit;
                                memset(vstr, '\0', maxstrlen);
                            }
                            if ( i > (into - ix) ) break;
                        }
                        break;
                    case Type::FIXED_LEN_BYTE_ARRAY:
                        if ( descr->type_length() > vtypes[j] ) {
                            sf_errprintf("Buffer (%d) too small; error parsing FixedLenByteArray.\n", vtypes[j]);
                            sf_errprintf("Group %d, row %d, col %d had a string of length %d.\n", r, i + ix, j, descr->type_length());
                            rc = 17103;
                            goto exit;
                        }
                        else {
                            flba_scanner = std::make_shared<parquet::FixedLenByteArrayScanner>(row_group_reader->Column(jsel));
                            while (flba_scanner->HasNext()) {
                                flba_scanner->NextValue(&vfixedlen, &is_null);
                                if ( i++ < (infrom - ix) ) continue;
                                if ( is_null ) {
                                    warn_strings++;
                                }
                                else {
                                    memcpy(vstr, vfixedlen.ptr, descr->type_length());
                                    // sf_printf_debug(debug, "\t(FLBA, %ld, %ld): %s\n", j, i + ix, vstr);
                                    if ( (rc = SF_sstore(j + 1, i + ix - infrom, vstr)) ) goto exit;
                                    memset(vstr, '\0', maxstrlen);
                                }
                                if ( i > (into - ix) ) break;
                            }
                        }
                        break;
                    default:
                        sf_errprintf("Unknown parquet type.\n");
                        rc = 17100;
                        goto exit;
                }
                ig = i > ig? i: ig;
            }
        }

        if ( warn_strings > 0 ) {
            sf_printf("Warning: %ld NaN values in string variables coerced to blanks ('').\n", warn_strings);
        }
        sf_running_timer (&timer, "Read data from disk");

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
    return(rc);
}
