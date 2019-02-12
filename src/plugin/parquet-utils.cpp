ST_retcode sf_scalar_dbl(char const *scalar, int64_t vlen, ST_double *z)
{
    ST_retcode rc = 0;
    SPARQUET_CHAR(vscalar, 32);
    memcpy(vscalar, scalar, vlen);
    if ( (rc = SF_scal_use(vscalar, z)) ) return (rc);
    return (rc);
}

ST_retcode sf_scalar_int(char const *scalar, int64_t vlen, int64_t *s)
{
    ST_retcode rc = 0;
    ST_double z;
    SPARQUET_CHAR(vscalar, 32);
    memcpy(vscalar, scalar, vlen);
    if ( (rc = SF_scal_use(vscalar, &z)) ) return (rc);
    *s = (int64_t) z;
    return (rc);
}

ST_retcode sf_matrix_int(char const *matrix, int64_t mlen, int64_t mcol, int64_t *m)
{
    ST_retcode rc = 0;
    ST_double z;
    int64_t j;

    SPARQUET_CHAR(vmatrix, 32);
    memcpy(vmatrix, matrix, mlen);

    for (j = 0; j < mcol; j++) {
        if ( (rc = SF_mat_el(vmatrix, 1, j + 1, &z)) ) return (rc);
        m[j] = (int64_t) z;
    }

    return (rc);
}

// Stata function: Low-level nrow and ncol
//
// parameters
//     fname - parquet file name
//
// matrices
//     __sparquet_rowgix
// scalars
//     __sparquet_nrow
//     __sparquet_ncol
//     __sparquet_ngroup
//     __sparquet_readrg

ST_retcode sf_ll_shape(const char *fname, const int debug)
{
    ST_retcode rc = 0, any_rc = 0;
    int64_t nrow, ncol, ngroup, _readrg, readrg, r, readrow = 0, nbytes = 0;
    SPARQUET_CHAR(vscalar, 32);

    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(fname, false);

        std::shared_ptr<parquet::FileMetaData> file_metadata =
            parquet_reader->metadata();

        // Basic Info
        // ----------

        ngroup = file_metadata->num_row_groups();
        nrow   = file_metadata->num_rows();
        ncol   = file_metadata->num_columns();

        if ( ngroup > 1 ) {
            sf_printf_debug(debug, "\tShape: %ld by %ld in %ld groups\n", nrow, ncol, ngroup);
        }
        else {
            sf_printf_debug(debug, "\tShape: %ld by %ld\n", nrow, ncol);
        }

        // Row groups
        // ----------

        if ( (rc = sf_scalar_int("__sparquet_readrg", 17, &readrg)) ) any_rc = rc;
        _readrg = readrg? readrg: 1;
        int64_t rowgix[_readrg];

        if ( (rc = sf_matrix_int("__sparquet_rowgix", 17, _readrg, rowgix)) ) any_rc = rc;
        for (r = 0; r < _readrg; r++)
            --rowgix[r];

        if ( any_rc ) {
            rc = any_rc;
            goto exit;
        }

        // Check row groups make sense
        // ---------------------------

        if ( readrg ) {
            for (r = 0; r < readrg; ++r) {
                if ( (rowgix[r] + 1) > ngroup ) {
                    sf_errprintf("Attempted to row group %ld but file only had %ld.\n",
                                 rowgix[r] + 1, ngroup);
                    rc = 17301;
                    goto exit;
                }
                else {
                    readrow += file_metadata->RowGroup(rowgix[r])->num_rows();
                }
            }
            sf_printf_debug(
                debug,
                "(note: reading %ld of %ld observations from %ld groups)\n",
                readrow, nrow, readrg
            );
            nrow = readrow;
            ngroup = readrg;
        }
        nbytes += filesize(fname);

        // Export info
        // -----------

        memcpy(vscalar, "__sparquet_nrow", 15);
        if ( (rc = SF_scal_save(vscalar, (ST_double) nrow)) ) goto exit;

        memcpy(vscalar, "__sparquet_ncol", 15);
        if ( (rc = SF_scal_save(vscalar, (ST_double) ncol)) ) goto exit;

        memcpy(vscalar, "__sparquet_ngroup", 17);
        if ( (rc = SF_scal_save(vscalar, (ST_double) ngroup)) ) goto exit;

        memcpy(vscalar, "__sparquet_nbytes", 17);
        if ( (rc = SF_scal_save(vscalar, (ST_double) nbytes)) ) goto exit;

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
    return (rc);
}

// Stata function: Low-level column names
//
// fname is the parquet file name
// fcols is the temporary text file where to write column names

ST_retcode sf_ll_colnames(
    const char *fname,
    const char *fcols,
    const int debug)
{
    ST_retcode rc = 0;
    int64_t ncol, j;

    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(fname, false);

        std::shared_ptr<parquet::FileMetaData> file_metadata =
            parquet_reader->metadata();

        ncol = file_metadata->num_columns();

        // Write column names to file; one per line
        std::ofstream fstream;
        fstream.open(fcols);
        for (j = 0; j < ncol; j++) {
            const parquet::ColumnDescriptor* descr =
                file_metadata->schema()->Column(j);
            fstream << descr->name() << "\n";
            sf_printf_debug(debug, "\tColumn %ld: %s\n", j, descr->name().c_str());
        }
        fstream.close();

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

    return (rc);
}

// Stata function: Low-level column types
//
// fname is the parquet file name
// strbuffer is the string length fallback
//
// matrices
//     __sparquet_coltypes
//     __sparquet_rawtypes
//     __sparquet_colix
//     __sparquet_rowgix
// scalars
//     __sparquet_strscan
//     __sparquet_ncol
//     __sparquet_into
//     __sparquet_infrom
//     __sparquet_readrg

ST_retcode sf_ll_coltypes(
    const char *fname,
    const uint64_t strbuffer,
    const int debug)
{
    ST_retcode rc = 0, any_rc = 0;
    bool is_null;
    clock_t timer = clock();
    int64_t strlen, nrow_groups, readrg, _readrg;
    int64_t strscan = 0, ncol = 1, infrom = 0, into = 0;
    int64_t rg, r, i, j, jsel;

    // First get all the shape scalars
    // -------------------------------

    if ( (rc = sf_scalar_int("__sparquet_strscan", 18, &strscan)) ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_ncol",    15, &ncol))    ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_infrom",  17, &infrom))  ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_into",    15, &into))    ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_readrg",  17, &readrg))  ) any_rc = rc;
    --infrom; --into; _readrg = readrg? readrg: 1;

    // Parse column and row group indexes
    // ----------------------------------

    int64_t vtypes[ncol];
    int64_t rtypes[ncol];
    int64_t colix[ncol];
    int64_t rowgix[_readrg];

    if ( (rc = sf_matrix_int("__sparquet_rowgix", 17, _readrg, rowgix)) ) any_rc = rc;
    for (r = 0; r < _readrg; r++)
        --rowgix[r];

    if ( (rc = sf_matrix_int("__sparquet_colix", 16, ncol, colix)) ) any_rc = rc;
    for (j = 0; j < ncol; j++)
        --colix[j];

    if ( any_rc ) {
        rc = any_rc;
        goto exit;
    }

    // Scan file!
    // ----------

    strscan += infrom;
    if ( strscan < into ) into = strscan;

    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(fname, false);

        std::shared_ptr<parquet::FileMetaData> file_metadata =
            parquet_reader->metadata();

        std::shared_ptr<parquet::RowGroupReader> row_group_reader;
        std::shared_ptr<parquet::ByteArrayScanner> ba_scanner;
        parquet::ByteArray vbytearray;

        nrow_groups = file_metadata->num_row_groups();

        // Check row groups make sense
        // ---------------------------

        if ( readrg ) {
            for (r = 0; r < readrg; ++r) {
                if ( (rowgix[r] + 1) > nrow_groups ) {
                    sf_errprintf("Attempted to row group %ld but file only had %ld.\n",
                                 rowgix[r] + 1, nrow_groups);
                    rc = 17301;
                    goto exit;
                }
            }
        }

        // Iterate over columns
        // --------------------

        for (j = 0; j < ncol; j++) {
            rg = 0;
            jsel = colix[j];
            const parquet::ColumnDescriptor* descr =
                file_metadata->schema()->Column(jsel);

            sf_printf_debug(debug, "\tColumn %ld: %s\n", jsel, descr->name().c_str());
            switch (descr->physical_type()) {
                case Type::BOOLEAN:    // byte
                    rtypes[j] = 1;
                    vtypes[j] = -1;
                    break;
                case Type::INT32:      // long
                    rtypes[j] = 2;
                    vtypes[j] = -3;
                    break;
                case Type::INT64:      // double
                    rtypes[j] = 3;
                    vtypes[j] = -5;
                    break;
                case Type::INT96:
                    rtypes[j] = 4;
                    sf_errprintf("96-bit integers not implemented.\n");
                    rc = 17101;
                    goto exit;
                case Type::FLOAT:      // float
                    rtypes[j] = 5;
                    vtypes[j] = -4;
                    break;
                case Type::DOUBLE:     // double
                    rtypes[j] = 6;
                    vtypes[j] = -5;
                    break;
                case Type::BYTE_ARRAY: // str#, strL
                    rtypes[j] = 7;
                    // vtypes[j] = SV_missval;
                    // Scan longest string length
                    if ( strscan > infrom ) {
                        i = 0;
                        strlen = 0;
                        for (r = 0; r < nrow_groups; ++r) {
                            if ( readrg ) {
                                if ( r == rowgix[rg] ) {
                                    rg++;
                                }
                                else {
                                    continue;
                                }
                            }
                            row_group_reader = parquet_reader->RowGroup(r);
                            ba_scanner = std::make_shared<parquet::ByteArrayScanner>(row_group_reader->Column(jsel));
                            while ( ba_scanner->HasNext() && i++ < infrom ) {
                                ba_scanner->NextValue(&vbytearray, &is_null);
                            }
                            i--;
                            while ( ba_scanner->HasNext() && i++ <= into ) {
                                ba_scanner->NextValue(&vbytearray, &is_null);
                                if (vbytearray.len > strlen) strlen = vbytearray.len;
                            }
                            if ( i >= into ) break;
                        }
                        vtypes[j] = strlen > 0? strlen: (i >= into? 1: strbuffer);
                    }
                    else {
                        vtypes[j] = strbuffer;
                    }
                    break;
                case Type::FIXED_LEN_BYTE_ARRAY: // str#, strL
                    rtypes[j] = 8;
                    vtypes[j] = descr->type_length();
                    break;
                default:
                    sf_errprintf("Unknown parquet type.\n");
                    rc = 17100;
                    goto exit;
            }
        }

        SPARQUET_CHAR(vmatrix, 32);
        memcpy(vmatrix, "__sparquet_coltypes", 19);
        for (j = 0; j < ncol; j++) {
            if ( (rc = SF_mat_store(vmatrix, 1, j + 1, vtypes[j])) ) goto exit;
        }

        memcpy(vmatrix, "__sparquet_rawtypes", 19);
        for (j = 0; j < ncol; j++) {
            if ( (rc = SF_mat_store(vmatrix, 1, j + 1, rtypes[j])) ) goto exit;
        }

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

    sf_running_timer (&timer, "Scanned column types");
exit:
    return (rc);
}
