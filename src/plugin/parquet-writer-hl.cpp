// Stata function: High-level write full varlist
// __sparquet_ncol must exist and be a numeric scalar with the # of cols
// __sparquet_coltypes must exist and be a 1 by # col matrix
// __sparquet_rg_size must exist and be a 1 by # col matrix
ST_retcode sf_hl_write_varlist(
    const char *fname,
    const char *fcols,
    const int verbose,
    const int debug,
    const int strbuffer)
{

    ST_retcode rc = 0, any_rc = 0;
    ST_double z;
    SPARQUET_CHAR(vmatrix, 32);
    SPARQUET_CHAR(vscalar, 32);
    SPARQUET_CHAR(vstr, strbuffer);

    int64_t in1 = SF_in1();
    int64_t in2 = SF_in2();
    int64_t N = in2 - in1 + 1;
    int64_t vtype, i, j, ncol = 1, rg_size = 16;
    clock_t timer = clock();

    std::string line;
    std::ifstream fstream;

    // Get column and type info from Stata
    // -----------------------------------

    memset(vscalar, '\0', 32);
    memcpy(vscalar, "__sparquet_rg_size", 18);
    if ( (rc = SF_scal_use(vscalar, &z)) ) {
        any_rc = rc;
    }
    else {
        rg_size = (int64_t) z;
    }

    memset(vscalar, '\0', 32);
    memcpy(vscalar, "__sparquet_ncol", 15);
    if ( (rc = SF_scal_use(vscalar, &z)) ) {
        any_rc = rc;
    }
    else {
        ncol = (int64_t) z;
    }
    sf_printf_debug(debug, "# columns: %ld\n", ncol);

    ST_double vtypes_dbl[ncol];
    int64_t vtypes[ncol];
    std::string vnames[ncol];

    std::vector<std::shared_ptr<arrow::Field>> vfields(ncol);
    std::vector<std::shared_ptr<arrow::Array>> varrays(ncol);

    memcpy(vmatrix, "__sparquet_coltypes", 19);
    for (j = 0; j < ncol; j++) {
        if ( (rc = SF_mat_el(vmatrix, 1, j + 1, vtypes_dbl + j)) ) goto exit;
        vtypes[j] = (int64_t) vtypes_dbl[j];
        // sf_printf_debug(2, "\tctype: %ld\n", vtypes[j]);
    }

    // Get variable names
    // ------------------

    if ( any_rc ) {
        rc = any_rc;
        goto exit;
    }

    j = 0;
    fstream.open(fcols);
    if ( fstream.is_open() ) {
        while ( std::getline(fstream, line) ) {
            vnames[j++] = line;
        }
        fstream.close();
    }
    else {
        sf_errprintf("Unable to read file '%s'\n", fcols);
        return(601);
    }

    // Write columns to file
    // ---------------------

    try {
        timer = clock();
        for (j = 0; j < ncol; j++) {
            vtype = vtypes[j];
            // sf_printf_debug(2, "col %ld: %ld (%s)\n", j, vtype, vnames[j].c_str());
            // TODO: Boolean is only 0/1; keep? Or always int32?
            // TODO: Missing values
            if ( vtype == -1 ) {
                arrow::BooleanBuilder boolbuilder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    PARQUET_THROW_NOT_OK(boolbuilder.Append((bool) z));
                }
                PARQUET_THROW_NOT_OK(boolbuilder.Finish(&varrays[j]));
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::boolean());
            }
            else if ( vtype == -2 ) {
                arrow::Int32Builder i32builder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    PARQUET_THROW_NOT_OK(i32builder.Append((int32_t) z));
                }
                PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::int32());
            }
            else if ( vtype == -3 ) {
                arrow::Int32Builder i32builder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    PARQUET_THROW_NOT_OK(i32builder.Append((int32_t) z));
                }
                PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::int32());
            }
            else if ( vtype == -4 ) {
                arrow::FloatBuilder f32builder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    PARQUET_THROW_NOT_OK(f32builder.Append((float) z));
                }
                PARQUET_THROW_NOT_OK(f32builder.Finish(&varrays[j]));
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::float32());
            }
            else if ( vtype == -5 ) {
                arrow::FloatBuilder f32builder;
                arrow::DoubleBuilder f64builder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    PARQUET_THROW_NOT_OK(f64builder.Append((float) z));
                }
                PARQUET_THROW_NOT_OK(f64builder.Finish(&varrays[j]));
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::float64());
            }
            else if ( vtype > 0 ) {
                arrow::StringBuilder strbuilder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_sdata(j + 1, i + in1, vstr)) ) goto exit;
                    PARQUET_THROW_NOT_OK(strbuilder.Append(vstr));
                    memset(vstr, '\0', strbuffer);
                }
                PARQUET_THROW_NOT_OK(strbuilder.Finish(&varrays[j]));
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::utf8());
            }
            else {
                sf_errprintf("Unsupported type.\n");
                rc = 17100;
                goto exit;
            }
        }
        sf_running_timer (&timer, "Copied data into Arrow table");

        std::shared_ptr<arrow::Schema> schema = arrow::schema(vfields);
        std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, varrays);

        // rg_size is, according to the source files, supposed to be
        // really large and controls how the file gets split.

        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        PARQUET_THROW_NOT_OK(
                arrow::io::FileOutputStream::Open(fname, &outfile));
        PARQUET_THROW_NOT_OK(
                parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, rg_size));

        // std::shared_ptr<parquet::arrow::FileWriter> writer;
        // PARQUET_THROW_NOT_OK(
        //     parquet::arrow::OpenFile(outfile, arrow::default_memory_pool(), &writer));
        // PARQUET_THROW_NOT_OK(writer->WriteTable(*table, rg_size));

        sf_running_timer (&timer, "Wrote table to file");
        sf_printf_debug(verbose, "\t%s\n", fname);
        sf_printf_debug(verbose, "\t%ld columns\n", ncol);
        sf_printf_debug(verbose, "\t%ld rows\n", N);

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
    return (rc);
}
