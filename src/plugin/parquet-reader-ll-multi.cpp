// Stata function: Low-level read full varlist
//
// matrices
//     __sparquet_coltypes
//     __sparquet_colix
// scalars
//     __sparquet_ncol
//     __sparquet_into
//     __sparquet_infrom

ST_retcode sf_ll_read_varlist_multi(
    const char *flist,
    const int verbose,
    const int debug,
    const uint64_t strbuffer)
{
    ST_retcode rc = 0, any_rc = 0;

    bool is_null;
    int64_t nrow, nrow_groups, maxstrlen;
    int64_t r, i, j, jsel, ix, ig;
    int64_t warn_strings = 0, ncol = 1, infrom = 0, into = 0, nfiles = 0, nread = 0;
    SPARQUET_CHAR(vscalar, 32);

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

        // Read selected columns; read in range
        if ( (rc = sf_scalar_int("__sparquet_ncol",   15, &ncol))   ) any_rc = rc;
        if ( (rc = sf_scalar_int("__sparquet_infrom", 17, &infrom)) ) any_rc = rc;
        if ( (rc = sf_scalar_int("__sparquet_into",   15, &into))   ) any_rc = rc;
        --into; --infrom;

        maxstrlen = 1;
        int64_t vtypes[ncol];
        int64_t colix[ncol];

        // Adjust column selection to be 0-indexed
        if ( (rc = sf_matrix_int("__sparquet_colix", 16, ncol, colix)) ) any_rc = rc;
        for (j = 0; j < ncol; j++)
            --colix[j];

        // Encoded variable types
        if ( (rc = sf_matrix_int("__sparquet_coltypes", 19, ncol, vtypes)) ) any_rc = rc;
        for (j = 0; j < ncol; j++)
            if ( vtypes[j] > maxstrlen ) maxstrlen = vtypes[j];

        SPARQUET_CHAR(vstr, maxstrlen);
        if ( any_rc ) {
            rc = any_rc;
            goto exit;
        }

        // Loop through files
        // ------------------

        std::shared_ptr<parquet::FileMetaData> file_metadata;
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader;

        std::string fname;
        std::ifstream fstream;
        fstream.open(flist);

        if ( fstream.is_open() ) {
            ix = ig = 0;
            clock_t timer = clock();
            while ( std::getline(fstream, fname) ) {
                parquet_reader = parquet::ParquetFileReader::OpenFile(fname, false);
                file_metadata  = parquet_reader->metadata();
                nrow_groups    = file_metadata->num_row_groups();

                sf_printf_debug(verbose, "\tFile: %s (%ld rows)\n",
                                fname.c_str(), file_metadata->num_rows());

                // Read all the observations in the file
                // -------------------------------------

                // For each group, loop through each column
                // For each column, loop through each row

                for (r = 0; r < nrow_groups; ++r) {
                    ix += ig;
                    ig = 0;
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
                                while ( bool_scanner->HasNext() && i++ < (infrom - ix) ) {
                                    bool_scanner->NextValue(&vbool, &is_null);
                                }
                                i--;
                                while ( bool_scanner->HasNext() && i++ <= (into - ix) ) {
                                    bool_scanner->NextValue(&vbool, &is_null);
                                    // sf_printf_debug(2, "\t(bool, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vbool);
                                    if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: (ST_double) vbool)) ) goto exit;
                                }
                                break;
                            case Type::INT32:      // long
                                int32_scanner = std::make_shared<parquet::Int32Scanner>(row_group_reader->Column(jsel));
                                while ( int32_scanner->HasNext() && i++ < (infrom - ix) ) {
                                    int32_scanner->NextValue(&vint32, &is_null);
                                }
                                i--;
                                while ( int32_scanner->HasNext() && i++ <= (into - ix) ) {
                                    int32_scanner->NextValue(&vint32, &is_null);
                                    // sf_printf_debug(2, "\t(int32, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vint32);
                                    if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: (ST_double) vint32)) ) goto exit;
                                }
                                break;
                            case Type::INT64:      // double
                                int64_scanner = std::make_shared<parquet::Int64Scanner>(row_group_reader->Column(jsel));
                                while ( int64_scanner->HasNext() && i++ < (infrom - ix) ) {
                                    int64_scanner->NextValue(&vint64, &is_null);
                                }
                                i--;
                                while ( int64_scanner->HasNext() && i++ <= (into - ix) ) {
                                    int64_scanner->NextValue(&vint64, &is_null);
                                    // sf_printf_debug(2, "\t(int64, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vint64);
                                    if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: (ST_double) vint64)) ) goto exit;
                                }
                                break;
                            case Type::INT96:
                                sf_errprintf("96-bit integers not implemented.\n");
                                rc = 17101;
                                goto exit;
                            case Type::FLOAT:      // float
                                float_scanner = std::make_shared<parquet::FloatScanner>(row_group_reader->Column(jsel));
                                while ( float_scanner->HasNext() && i++ < (infrom - ix) ) {
                                    float_scanner->NextValue(&vfloat, &is_null);
                                }
                                i--;
                                while ( float_scanner->HasNext() && i++ <= (into - ix) ) {
                                    float_scanner->NextValue(&vfloat, &is_null);
                                    // sf_printf_debug(2, "\t(float, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vfloat);
                                    if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: (ST_double) vfloat)) ) goto exit;
                                }
                                break;
                            case Type::DOUBLE:     // double
                                double_scanner = std::make_shared<parquet::DoubleScanner>(row_group_reader->Column(jsel));
                                while ( double_scanner->HasNext() && i++ < (infrom - ix) ) {
                                    double_scanner->NextValue(&vdouble, &is_null);
                                }
                                i--;
                                while ( double_scanner->HasNext() && i++ <= (into - ix) ) {
                                    double_scanner->NextValue(&vdouble, &is_null);
                                    // sf_printf_debug(debug, "\t(double, %ld, %ld): %9.4f\n", j, i + ix, (ST_double) vdouble);
                                    if ( (rc = SF_vstore(j + 1, i + ix - infrom, is_null? SV_missval: vdouble)) ) goto exit;
                                }
                                break;
                            case Type::BYTE_ARRAY: // str#, strL
                                ba_scanner = std::make_shared<parquet::ByteArrayScanner>(row_group_reader->Column(jsel));
                                while ( ba_scanner->HasNext() && i++ < (infrom - ix) ) {
                                    ba_scanner->NextValue(&vbytearray, &is_null);
                                }
                                i--;
                                while ( ba_scanner->HasNext() && i++ <= (into - ix) ) {
                                    ba_scanner->NextValue(&vbytearray, &is_null);
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
                                    while ( flba_scanner->HasNext() && i++ < (infrom - ix) ) {
                                        flba_scanner->NextValue(&vfixedlen, &is_null);
                                    }
                                    i--;
                                    while ( flba_scanner->HasNext() && i++ <= (into - ix) ) {
                                        flba_scanner->NextValue(&vfixedlen, &is_null);
                                        if ( is_null ) {
                                            warn_strings++;
                                        }
                                        else {
                                            memcpy(vstr, vfixedlen.ptr, descr->type_length());
                                            // sf_printf_debug(debug, "\t(FLBA, %ld, %ld): %s\n", j, i + ix, vstr);
                                            if ( (rc = SF_sstore(j + 1, i + ix - infrom, vstr)) ) goto exit;
                                            memset(vstr, '\0', maxstrlen);
                                        }
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
                    nread += ig;
                }
                ++nfiles;
                if ( ix > into ) break;
            }
            fstream.close();
            if ( warn_strings > 0 ) {
                sf_printf("Warning: %ld NaN values in string variables coerced to blanks ('').\n", warn_strings);
            }
            sf_running_timer(&timer, "Read data from disk");
        }

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

    if ( (rc = sf_scalar_int("__sparquet_nrow", 15, &nrow)) ) goto exit;
    if ( nrow != nread ) {
        sf_errprintf("Warning: Expected %ld obs but only found %ld",
                     nrow, nread);
    }

    memcpy(vscalar, "__sparquet_nread", 16);
    if ( (rc = SF_scal_save(vscalar, (ST_double) nread)) ) goto exit;

exit:
    return(rc);
}
