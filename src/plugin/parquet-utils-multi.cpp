// Stata function: Low-level nrow and ncol
//
// parameters
//     flist - parquet file list
//
// scalars
//     __sparquet_nrow
//     __sparquet_ncol

ST_retcode sf_ll_shape_multi(const char *flist, const int debug)
{
    ST_retcode rc = 0;
    int64_t nrow = 0, ncol = 0, nfiles = 0, fnrow, fncol;
    SPARQUET_CHAR(vscalar, 32);

    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader;
        std::shared_ptr<parquet::FileMetaData> file_metadata;

        std::string fname;
        std::ifstream fstream;
        fstream.open(flist);

        if ( fstream.is_open() ) {
            while ( std::getline(fstream, fname) ) {
                parquet_reader = parquet::ParquetFileReader::OpenFile(fname, false);
                file_metadata  = parquet_reader->metadata();
                fnrow = file_metadata->num_rows();
                fncol = file_metadata->num_columns();
                if ( nfiles++ > 1 ) {
                    if ( fncol != ncol ) {
                        sf_errprintf("File #%ld had %ld columns (expected %ld)\n",
                                     nfiles, fncol, ncol);
                        rc = 198;
                        goto exit;
                    }
                }
                else {
                    ncol = fncol;
                }
                nrow += fnrow;
            }
            fstream.close();
        }
        else {
            sf_errprintf("Unable to read files in folder\n");
            return(601);
        }

        // sf_printf_debug(2, "Shape: %ld by %ld\n", nrow, ncol);

        memcpy(vscalar, "__sparquet_nrow", 15);
        if ( (rc = SF_scal_save(vscalar, (ST_double) nrow)) ) goto exit;
        memcpy(vscalar, "__sparquet_ncol", 15);
        if ( (rc = SF_scal_save(vscalar, (ST_double) ncol)) ) goto exit;

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
    return (rc);
}

// Stata function: Low-level column names
//
// flist is the file with the list of parquet files
// fcols is the temporary text file where to write column names

ST_retcode sf_ll_colnames_multi(
    const char *flist,
    const char *fcols,
    const int debug)
{
    ST_retcode rc = 0;
    int64_t ncol = 0, nfiles = 0, fncol, j;
    rc = sf_scalar_int("__sparquet_ncol", 15, &ncol);
    std::string vnames[ncol];
    if ( rc ) goto exit;

    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader;
        std::shared_ptr<parquet::FileMetaData> file_metadata;

        std::string fname;
        std::ifstream fstream;
        std::ofstream fcolstream;
        fstream.open(flist);

        if ( fstream.is_open() ) {
            while ( std::getline(fstream, fname) ) {
                parquet_reader = parquet::ParquetFileReader::OpenFile(fname, false);
                file_metadata  = parquet_reader->metadata();
                fncol = file_metadata->num_columns();
                if ( nfiles++ > 1 ) {
                    if ( fncol != ncol ) {
                        sf_errprintf("File #%ld had %ld columns (expected %ld)\n",
                                     nfiles, fncol, ncol);
                        rc = 198;
                    }
                    for (j = 0; j < ncol; j++) {
                        const parquet::ColumnDescriptor* descr =
                            file_metadata->schema()->Column(j);
                        if ( strcmp(vnames[j].c_str(), descr->name().c_str()) ) {
                            sf_errprintf("Column %ld (%s) not in previous file",
                                         j, descr->name().c_str());
                            rc = 198;
                        }
                    }
                    if ( rc ) goto exit;
                }
                else {
                    ncol = fncol;

                    // Write column names to file; one per line
                    fcolstream.open(fcols);
                    for (j = 0; j < ncol; j++) {
                        const parquet::ColumnDescriptor* descr =
                            file_metadata->schema()->Column(j);

                        vnames[j] = descr->name();
                        fcolstream << descr->name() << "\n";
                    }
                    fcolstream.close();
                }
            }
            fstream.close();
        }

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
    return (rc);
}

// Stata function: Low-level column types
//
// flist is the parquet file with the lsit of names
// strbuffer is the string length fallback
//
// matrices
//     __sparquet_coltypes
//     __sparquet_colix
// scalars
//     __sparquet_strscan
//     __sparquet_ncol
//     __sparquet_into
//     __sparquet_infrom

ST_retcode sf_ll_coltypes_multi(
    const char *flist,
    const uint64_t strbuffer,
    const int debug)
{
    ST_retcode rc = 0, any_rc = 0;
    bool is_null;
    clock_t timer = clock();
    int64_t vtype, strlen, nrow_groups;
    int64_t strscan = 0, ncol = 1, infrom = 0, into = 0, nfiles = 0;
    int64_t r, *i, j, jsel;

    if ( (rc = sf_scalar_int("__sparquet_strscan", 18, &strscan)) ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_ncol",    15, &ncol))    ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_infrom",  17, &infrom))  ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_into",    15, &into))    ) any_rc = rc;
    --infrom; --into;

    int64_t vtypes[ncol];
    int64_t colix[ncol];
    int64_t obs[ncol];
    if ( (rc = sf_matrix_int("__sparquet_colix", 16, ncol, colix)) ) any_rc = rc;
    for (j = 0; j < ncol; j++)
        --colix[j];

    for (j = 0; j < ncol; j++)
        obs[j] = vtypes[j] = 0;

    if ( any_rc ) {
        rc = any_rc;
        goto exit;
    }

    strscan += infrom;
    if ( strscan < into ) into = strscan;

    try {
        std::shared_ptr<parquet::FileMetaData> file_metadata;
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader;

        std::shared_ptr<parquet::RowGroupReader> row_group_reader;
        std::shared_ptr<parquet::ByteArrayScanner> ba_scanner;
        parquet::ByteArray vbytearray;

        std::string fname;
        std::ifstream fstream;
        fstream.open(flist);

        if ( fstream.is_open() ) {
            while ( std::getline(fstream, fname) ) {
                parquet_reader = parquet::ParquetFileReader::OpenFile(fname, false);
                file_metadata  = parquet_reader->metadata();
                nrow_groups    = file_metadata->num_row_groups();
                ncol = file_metadata->num_columns();
                for (j = 0; j < ncol; j++) {
                    jsel = colix[j];
                    const parquet::ColumnDescriptor* descr =
                        file_metadata->schema()->Column(jsel);

                    // sf_printf_debug(2, "\tColumn %ld: %s\n", jsel, descr->name().c_str());
                    switch (descr->physical_type()) {
                        case Type::BOOLEAN:    // byte
                            if ( nfiles == 0 ) {
                                vtypes[j] = -1;
                            }
                            else if ( vtypes[j] != -1 ) {
                                sf_errprintf("Inconsistent type for column %ld.\n", j);
                                rc = 17201;
                                goto exit;
                            }
                            break;
                        case Type::INT32:      // long
                            if ( nfiles == 0 ) {
                                vtypes[j] = -3;
                            }
                            else if ( vtypes[j] != -3 ) {
                                sf_errprintf("Inconsistent type for column %ld.\n", j);
                                rc = 17201;
                                goto exit;
                            }
                            break;
                        case Type::INT64:      // double
                            if ( nfiles == 0 ) {
                                vtypes[j] = -5;
                            }
                            else if ( vtypes[j] != -5 ) {
                                sf_errprintf("Inconsistent type for column %ld.\n", j);
                                rc = 17201;
                                goto exit;
                            }
                            break;
                        case Type::INT96:
                            sf_errprintf("96-bit integers not implemented.\n");
                            rc = 17101;
                            goto exit;
                        case Type::FLOAT:      // float
                            if ( nfiles == 0 ) {
                                vtypes[j] = -4;
                            }
                            else if ( vtypes[j] != -4 ) {
                                sf_errprintf("Inconsistent type for column %ld.\n", j);
                                rc = 17201;
                                goto exit;
                            }
                            break;
                        case Type::DOUBLE:     // double
                            if ( nfiles == 0 ) {
                                vtypes[j] = -5;
                            }
                            else if ( vtypes[j] != -5 ) {
                                sf_errprintf("Inconsistent type for column %ld.\n", j);
                                rc = 17201;
                                goto exit;
                            }
                            break;
                        case Type::BYTE_ARRAY: // str#, strL
                            // vtypes[j] = SV_missval;
                            // Scan longest string length
                            if ( strscan > infrom ) {
                                i = obs + j;
                                strlen = 0;
                                for (r = 0; r < nrow_groups; ++r) {
                                    row_group_reader = parquet_reader->RowGroup(r);
                                    ba_scanner = std::make_shared<parquet::ByteArrayScanner>(row_group_reader->Column(jsel));
                                    while ( ba_scanner->HasNext() && (*i)++ < infrom ) {
                                        ba_scanner->NextValue(&vbytearray, &is_null);
                                    }
                                    (*i)--;
                                    while ( ba_scanner->HasNext() && (*i)++ <= into ) {
                                        ba_scanner->NextValue(&vbytearray, &is_null);
                                        if (vbytearray.len > strlen) strlen = vbytearray.len;
                                    }
                                    if ( (*i) >= into ) break;
                                }
                                vtype = strlen > 0? strlen: ((*i) >= into? 1: strbuffer);
                                if ( vtype > vtypes[j] ) vtypes[j] = vtype;
                            }
                            else {
                                if ( nfiles == 0 ) {
                                    vtypes[j] = strbuffer;
                                }
                                else if ( vtypes[j] != (int64_t) strbuffer ) {
                                    sf_errprintf("Inconsistent type for column %ld.\n", j);
                                    rc = 17201;
                                    goto exit;
                                }
                            }
                            break;
                        case Type::FIXED_LEN_BYTE_ARRAY: // str#, strL
                            if ( nfiles == 0 ) {
                                vtypes[j] = descr->type_length();
                            }
                            else if ( vtypes[j] != descr->type_length() ) {
                                sf_errprintf("Inconsistent type for column %ld.\n", j);
                                rc = 17201;
                                goto exit;
                            }
                            break;
                        default:
                            sf_errprintf("Unknown parquet type.\n");
                            rc = 17100;
                            goto exit;
                    }
                }
                ++nfiles;
            }
            fstream.close();
        }

        SPARQUET_CHAR(vmatrix, 32);
        memcpy(vmatrix, "__sparquet_coltypes", 19);
        for (j = 0; j < ncol; j++) {
            if ( (rc = SF_mat_store(vmatrix, 1, j + 1, vtypes[j])) ) goto exit;
        }

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

    sf_running_timer (&timer, "Scanned column types");
exit:
    return (rc);
}
