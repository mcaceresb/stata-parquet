// Stata function: Low-level nrow and ncol
// fname is the parquet file name
// __sparquet_nrow must exist in Stata and be a numeric scalar
// __sparquet_ncol must exist in Stata and be a numeric scalar
ST_retcode sf_ll_shape(const char *fname, const int debug)
{
    ST_retcode rc = 0;
    int64_t nrow, ncol;
    SPARQUET_CHAR(vscalar, 32);

    // File reader
    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(fname, false);

        std::shared_ptr<parquet::FileMetaData> file_metadata =
            parquet_reader->metadata();

        ncol = file_metadata->num_columns();
        nrow = file_metadata->num_rows();

        sf_printf_debug(debug, "\tShape: %ld by %ld\n", nrow, ncol);

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

// Stata function: Low-level column types and names
// fname is the parquet file name
// fcols is the temporary text file where to write column names
// __sparquet_coltypes must exist in Stata and be 1 by # cols
ST_retcode sf_ll_coltypes(
    const char *fname,
    const char *fcols,
    const int debug,
    const uint64_t strbuffer)
{
    ST_retcode rc = 0;
    int64_t ncol, j;
    SPARQUET_CHAR(vmatrix, 32);

    // File reader
    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(fname, false);

        std::shared_ptr<parquet::FileMetaData> file_metadata =
            parquet_reader->metadata();

        ncol = file_metadata->num_columns();
        double vtypes[ncol];

        std::ofstream fstream;
        fstream.open(fcols);
        for (j = 0; j < ncol; j++) {
            const parquet::ColumnDescriptor* descr =
                file_metadata->schema()->Column(j);

            fstream << descr->name() << "\n";
            sf_printf_debug(debug, "\tColumn %ld: %s\n", j, descr->name().c_str());
            switch (descr->physical_type()) {
                case Type::BOOLEAN:    // byte
                    vtypes[j] = -1;
                    break;
                case Type::INT32:      // long
                    vtypes[j] = -3;
                    break;
                case Type::INT64:      // double
                    vtypes[j] = -5;
                    break;
                case Type::INT96:
                    sf_errprintf("96-bit integers not implemented.\n");
                    rc = 17101;
                    goto exit;
                case Type::FLOAT:      // float
                    vtypes[j] = -4;
                    break;
                case Type::DOUBLE:     // double
                    vtypes[j] = -5;
                    break;
                case Type::BYTE_ARRAY: // str#, strL
                    // vtypes[j] = SV_missval;
                    vtypes[j] = strbuffer;
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
        }
        fstream.close();

        memcpy(vmatrix, "__sparquet_coltypes", 19);
        for (j = 0; j < ncol; j++) {
            if ( (rc = SF_mat_store(vmatrix, 1, j + 1, vtypes[j])) ) goto exit;
        }

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
    return (rc);
}
