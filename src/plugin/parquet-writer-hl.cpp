// Stata function: High-level write full varlist
//
// matrix
//     __sparquet_coltypes
// scalars
//     __sparquet_ncol
//     __sparquet_rg_size
//     __sparquet_chunkbytes
//     __sparquet_progress
//     __sparquet_check
ST_retcode sf_hl_write_varlist(
    const char *fname,
    const char *fcols,
    const int verbose,
    const int debug,
    const int strbuffer)
{

    ST_double z, progress;
    ST_retcode rc = 0, any_rc = 0;
    SPARQUET_CHAR(vstr, strbuffer);

    int64_t in1 = SF_in1();
    int64_t in2 = SF_in2();
    int64_t N = in2 - in1 + 1;
    int64_t vtype, i, j, ttot, tread, tevery, ncol = 1, rg_size = 16;
    int64_t arraybytes = 0;
    int64_t chunkbytes = 1073741824;
    int64_t warn_extended = 0;
    clock_t timer = clock();
    clock_t stimer = clock();

    std::string line;
    std::ifstream fstream;

    // Get column and type info from Stata
    // -----------------------------------

    if ( (rc = sf_scalar_int("__sparquet_chunkbytes", 21, &chunkbytes)) ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_rg_size",    18, &rg_size))    ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_ncol",       15, &ncol))       ) any_rc = rc;
    if ( (rc = sf_scalar_dbl("__sparquet_progress",   19, &progress))   ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_check",      16, &tevery))     ) any_rc = rc;

    sf_printf_debug(debug, "# columns: %ld\n", ncol);

    tread = 0;
    ttot = ncol * N;
    int64_t vtypes[ncol];
    std::string vnames[ncol];

    std::vector<std::shared_ptr<arrow::Field>> vfields(ncol);
    std::vector<std::shared_ptr<arrow::Array>> varrays(ncol);
    std::vector<std::shared_ptr<arrow::Column>> vcols(ncol);

    if ( (rc = sf_matrix_int("__sparquet_coltypes", 19, ncol, vtypes)) ) any_rc = rc;

    // Get variable names
    // ------------------

    if ( any_rc ) {
        rc = any_rc;
        goto exit;
    }

    if ( N == 0 ) {
        sf_errprintf("No observations\n");
        rc = 2000;
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
            arraybytes = 0;
            std::vector<std::shared_ptr<arrow::Array>> chunks;
            // sf_printf_debug(2, "col %ld: %ld (%s)\n", j, vtype, vnames[j].c_str());
            if ( vtype == -1 ) {
                // TODO: Boolean is only 0/1; keep? Or always int32?
                arrow::BooleanBuilder boolbuilder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    if ( z < SV_missval ) {
                        PARQUET_THROW_NOT_OK(boolbuilder.Append((bool) z));
                    }
                    else {
                        PARQUET_THROW_NOT_OK(boolbuilder.AppendNull());
                        if ( z > SV_missval ) {
                            ++warn_extended;
                        }
                    }
                    arraybytes += sizeof(bool);
                    if ( arraybytes > chunkbytes ) {
                        PARQUET_THROW_NOT_OK(boolbuilder.Finish(&varrays[j]));
                        chunks.push_back(std::move(varrays[j]));
                        boolbuilder.Reset();
                        arraybytes = 0;
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(boolbuilder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::boolean());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype == -2 ) {
                arrow::Int32Builder i32builder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    if ( z < SV_missval ) {
                        PARQUET_THROW_NOT_OK(i32builder.Append((int32_t) z));
                    }
                    else {
                        PARQUET_THROW_NOT_OK(i32builder.AppendNull());
                        if ( z > SV_missval ) {
                            ++warn_extended;
                        }
                    }
                    arraybytes += sizeof(int32_t);
                    if ( arraybytes > chunkbytes ) {
                        PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                        chunks.push_back(std::move(varrays[j]));
                        i32builder.Reset();
                        arraybytes = 0;
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::int32());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype == -3 ) {
                arrow::Int32Builder i32builder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    if ( z < SV_missval ) {
                        PARQUET_THROW_NOT_OK(i32builder.Append((int32_t) z));
                    }
                    else {
                        PARQUET_THROW_NOT_OK(i32builder.AppendNull());
                        if ( z > SV_missval ) {
                            ++warn_extended;
                        }
                    }
                    arraybytes += sizeof(int32_t);
                    if ( arraybytes > chunkbytes ) {
                        PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                        chunks.push_back(std::move(varrays[j]));
                        i32builder.Reset();
                        arraybytes = 0;
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::int32());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype == -4 ) {
                arrow::FloatBuilder f32builder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    if ( z < SV_missval ) {
                        PARQUET_THROW_NOT_OK(f32builder.Append((float) z));
                    }
                    else {
                        PARQUET_THROW_NOT_OK(f32builder.AppendNull());
                        if ( z > SV_missval ) {
                            ++warn_extended;
                        }
                    }
                    arraybytes += sizeof(float);
                    if ( arraybytes > chunkbytes ) {
                        PARQUET_THROW_NOT_OK(f32builder.Finish(&varrays[j]));
                        chunks.push_back(std::move(varrays[j]));
                        f32builder.Reset();
                        arraybytes = 0;
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(f32builder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::float32());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype == -5 ) {
                arrow::DoubleBuilder f64builder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    if ( z < SV_missval ) {
                        PARQUET_THROW_NOT_OK(f64builder.Append((double) z));
                    }
                    else {
                        PARQUET_THROW_NOT_OK(f64builder.AppendNull());
                        if ( z > SV_missval ) {
                            ++warn_extended;
                        }
                    }
                    arraybytes += sizeof(double);
                    if ( arraybytes > chunkbytes ) {
                        PARQUET_THROW_NOT_OK(f64builder.Finish(&varrays[j]));
                        chunks.push_back(std::move(varrays[j]));
                        f64builder.Reset();
                        arraybytes = 0;
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(f64builder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::float64());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype > 0 ) {
                arrow::StringBuilder strbuilder;
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_sdata(j + 1, i + in1, vstr)) ) goto exit;
                    PARQUET_THROW_NOT_OK(strbuilder.Append(vstr));
                    arraybytes += std::strlen(vstr) * sizeof(vstr);
                    memset(vstr, '\0', strbuffer);
                    if ( arraybytes > chunkbytes ) {
                        PARQUET_THROW_NOT_OK(strbuilder.Finish(&varrays[j]));
                        chunks.push_back(std::move(varrays[j]));
                        strbuilder.Reset();
                        arraybytes = 0;
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(strbuilder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::utf8());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else {
                sf_errprintf("Unsupported type.\n");
                rc = 17100;
                goto exit;
            }
        }
        sf_running_timer (&timer, "Copied data into Arrow table");

        std::shared_ptr<arrow::Schema> schema = arrow::schema(vfields);
        std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, vcols);

        // rg_size is, according to the source files, supposed to be
        // really large and controls how the file gets split.

        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        PARQUET_THROW_NOT_OK(
                arrow::io::FileOutputStream::Open(fname, &outfile));
        PARQUET_THROW_NOT_OK(
                parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, rg_size));

        sf_running_timer (&timer, "Wrote table to file");
        sf_printf_debug(verbose, "\t%s\n",          fname);
        sf_printf_debug(verbose, "\t%ld columns\n", ncol);
        sf_printf_debug(verbose, "\t%ld rows\n",    N);

        if ( warn_extended > 0 ) {
            sf_printf("Warning: %ld extended missing values coerced to NULL.\n", warn_extended);
        }

    } catch (const std::exception& e) {
        sf_errprintf("Parquet write error: %s\n", e.what());
        return(-1);
    }

exit:
    return (rc);
}

// Stata function: High-level write full varlist with if condition
//
// matrix
//     __sparquet_coltypes
// scalars
//     __sparquet_ncol
//     __sparquet_rg_size
//     __sparquet_chunkbytes
//     __sparquet_progress
//     __sparquet_check
ST_retcode sf_hl_write_varlist_if(
    const char *fname,
    const char *fcols,
    const int verbose,
    const int debug,
    const int strbuffer)
{

    ST_double z, progress;
    ST_retcode rc = 0, any_rc = 0;
    SPARQUET_CHAR(vstr, strbuffer);

    int64_t in1 = SF_in1();
    int64_t in2 = SF_in2();
    int64_t N = in2 - in1 + 1;
    int64_t vtype, i, j, ttot, tread, tevery, ncol = 1, rg_size = 16;
    int64_t arraybytes = 0;
    int64_t chunkbytes = 1073741824;
    int64_t warn_extended = 0;
    clock_t timer = clock();
    clock_t stimer = clock();

    std::string line;
    std::ifstream fstream;

    // Get column and type info from Stata
    // -----------------------------------

    if ( (rc = sf_scalar_int("__sparquet_chunkbytes", 21, &chunkbytes)) ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_rg_size",    18, &rg_size))    ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_ncol",       15, &ncol))       ) any_rc = rc;
    if ( (rc = sf_scalar_dbl("__sparquet_progress",   19, &progress))   ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_check",      16, &tevery))     ) any_rc = rc;

    sf_printf_debug(debug, "# columns: %ld\n", ncol);

    tread = 0;
    ttot = ncol * N;
    int64_t vtypes[ncol];
    std::string vnames[ncol];

    std::vector<std::shared_ptr<arrow::Field>> vfields(ncol);
    std::vector<std::shared_ptr<arrow::Array>> varrays(ncol);
    std::vector<std::shared_ptr<arrow::Column>> vcols(ncol);

    if ( (rc = sf_matrix_int("__sparquet_coltypes", 19, ncol, vtypes)) ) any_rc = rc;

    // Get variable names
    // ------------------

    if ( any_rc ) {
        rc = any_rc;
        goto exit;
    }

    if ( N == 0 ) {
        sf_errprintf("No observations\n");
        rc = 2000;
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
            arraybytes = 0;
            std::vector<std::shared_ptr<arrow::Array>> chunks;
            // sf_printf_debug(2, "col %ld: %ld (%s)\n", j, vtype, vnames[j].c_str());
            if ( vtype == -1 ) {
                // TODO: Boolean is only 0/1; keep? Or always int32?
                arrow::BooleanBuilder boolbuilder;
                for (i = 0; i < N; i++) {
                    if ( SF_ifobs(i + in1) ) {
                        if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                        if ( z < SV_missval ) {
                            PARQUET_THROW_NOT_OK(boolbuilder.Append((bool) z));
                        }
                        else {
                            PARQUET_THROW_NOT_OK(boolbuilder.AppendNull());
                            if ( z > SV_missval ) {
                                ++warn_extended;
                            }
                        }
                        arraybytes += sizeof(bool);
                        if ( arraybytes > chunkbytes ) {
                            PARQUET_THROW_NOT_OK(boolbuilder.Finish(&varrays[j]));
                            chunks.push_back(std::move(varrays[j]));
                            boolbuilder.Reset();
                            arraybytes = 0;
                        }
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(boolbuilder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::boolean());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype == -2 ) {
                arrow::Int32Builder i32builder;
                for (i = 0; i < N; i++) {
                    if ( SF_ifobs(i + in1) ) {
                        if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                        if ( z < SV_missval ) {
                            PARQUET_THROW_NOT_OK(i32builder.Append((int32_t) z));
                        }
                        else {
                            PARQUET_THROW_NOT_OK(i32builder.AppendNull());
                            if ( z > SV_missval ) {
                                ++warn_extended;
                            }
                        }
                        arraybytes += sizeof(int32_t);
                        if ( arraybytes > chunkbytes ) {
                            PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                            chunks.push_back(std::move(varrays[j]));
                            i32builder.Reset();
                            arraybytes = 0;
                        }
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::int32());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype == -3 ) {
                arrow::Int32Builder i32builder;
                for (i = 0; i < N; i++) {
                    if ( SF_ifobs(i + in1) ) {
                        if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                        if ( z < SV_missval ) {
                            PARQUET_THROW_NOT_OK(i32builder.Append((int32_t) z));
                        }
                        else {
                            PARQUET_THROW_NOT_OK(i32builder.AppendNull());
                            if ( z > SV_missval ) {
                                ++warn_extended;
                            }
                        }
                        arraybytes += sizeof(int32_t);
                        if ( arraybytes > chunkbytes ) {
                            PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                            chunks.push_back(std::move(varrays[j]));
                            i32builder.Reset();
                            arraybytes = 0;
                        }
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(i32builder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::int32());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype == -4 ) {
                arrow::FloatBuilder f32builder;
                for (i = 0; i < N; i++) {
                    if ( SF_ifobs(i + in1) ) {
                        if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                        if ( z < SV_missval ) {
                            PARQUET_THROW_NOT_OK(f32builder.Append((float) z));
                        }
                        else {
                            PARQUET_THROW_NOT_OK(f32builder.AppendNull());
                            if ( z > SV_missval ) {
                                ++warn_extended;
                            }
                        }
                        arraybytes += sizeof(float);
                        if ( arraybytes > chunkbytes ) {
                            PARQUET_THROW_NOT_OK(f32builder.Finish(&varrays[j]));
                            chunks.push_back(std::move(varrays[j]));
                            f32builder.Reset();
                            arraybytes = 0;
                        }
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(f32builder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::float32());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype == -5 ) {
                arrow::DoubleBuilder f64builder;
                for (i = 0; i < N; i++) {
                    if ( SF_ifobs(i + in1) ) {
                        if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                        if ( z < SV_missval ) {
                            PARQUET_THROW_NOT_OK(f64builder.Append((double) z));
                        }
                        else {
                            PARQUET_THROW_NOT_OK(f64builder.AppendNull());
                            if ( z > SV_missval ) {
                                ++warn_extended;
                            }
                        }
                        arraybytes += sizeof(double);
                        if ( arraybytes > chunkbytes ) {
                            PARQUET_THROW_NOT_OK(f64builder.Finish(&varrays[j]));
                            chunks.push_back(std::move(varrays[j]));
                            f64builder.Reset();
                            arraybytes = 0;
                        }
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(f64builder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::float64());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else if ( vtype > 0 ) {
                arrow::StringBuilder strbuilder;
                for (i = 0; i < N; i++) {
                    if ( SF_ifobs(i + in1) ) {
                        if ( (rc = SF_sdata(j + 1, i + in1, vstr)) ) goto exit;
                        PARQUET_THROW_NOT_OK(strbuilder.Append(vstr));
                        arraybytes += std::strlen(vstr) * sizeof(vstr);
                        memset(vstr, '\0', strbuffer);
                        if ( arraybytes > chunkbytes ) {
                            PARQUET_THROW_NOT_OK(strbuilder.Finish(&varrays[j]));
                            chunks.push_back(std::move(varrays[j]));
                            strbuilder.Reset();
                            arraybytes = 0;
                        }
                    }
                    if ( i % tevery == 0 ) {
                        tread += tevery;
                        sf_running_progress_write(
                            &timer,
                            &stimer,
                            progress,
                            j + 1, ncol,
                            i + 1, N,
                            100 * tread / ttot
                        );
                    }
                }
                if ( arraybytes ) {
                    PARQUET_THROW_NOT_OK(strbuilder.Finish(&varrays[j]));
                    chunks.push_back(std::move(varrays[j]));
                }
                vfields[j] = arrow::field(vnames[j].c_str(), arrow::utf8());
                vcols[j]   = std::make_shared<arrow::Column>(vfields[j], std::make_shared<arrow::ChunkedArray>(std::move(chunks)));;
            }
            else {
                sf_errprintf("Unsupported type.\n");
                rc = 17100;
                goto exit;
            }
        }
        sf_running_timer (&timer, "Copied data into Arrow table");

        std::shared_ptr<arrow::Schema> schema = arrow::schema(vfields);
        std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, vcols);

        // rg_size is, according to the source files, supposed to be
        // really large and controls how the file gets split.

        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        PARQUET_THROW_NOT_OK(
                arrow::io::FileOutputStream::Open(fname, &outfile));
        PARQUET_THROW_NOT_OK(
                parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, rg_size));

        sf_running_timer (&timer, "Wrote table to file");
        sf_printf_debug(verbose, "\t%s\n",          fname);
        sf_printf_debug(verbose, "\t%ld columns\n", ncol);
        sf_printf_debug(verbose, "\t%ld rows\n",    N);

        if ( warn_extended > 0 ) {
            sf_printf("Warning: %ld extended missing values coerced to NULL.\n", warn_extended);
        }

    } catch (const std::exception& e) {
        sf_errprintf("Parquet write error: %s\n", e.what());
        return(-1);
    }

exit:
    return (rc);
}
