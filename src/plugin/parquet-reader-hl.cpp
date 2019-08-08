// Stata function: High-level read full varlist
//
// matrices
//     __sparquet_coltypes
//     __sparquet_colix
//     __sparquet_rowgix
// scalars
//     __sparquet_ncol
//     __sparquet_threads
//     __sparquet_into
//     __sparquet_infrom
//     __sparquet_readrg
//     __sparquet_progress
//     __sparquet_check

ST_retcode sf_hl_read_varlist(
    const char *fname,
    const int verbose,
    const int debug,
    const uint64_t strbuffer)
{
    ST_double z, progress;
    ST_retcode rc = 0, any_rc = 0;
    clock_t timer = clock();
    clock_t stimer = clock();
    int64_t r, i, j, c, ig, ix, ic, ir, readrg, _readrg, ngroup;
    int64_t nfields, narrfrom, narrlen, nchunks, tobs, ttot, tevery, tread;
    int64_t maxstrlen = 1, nthreads = 1, ncol = 1, infrom = 1, into = 1, nread = 0;

    // int64_t vtype;
    SPARQUET_CHAR(vmatrix, 32);
    SPARQUET_CHAR(vscalar, 32);

    if ( (rc = sf_scalar_int("__sparquet_threads",  18, &nthreads)) ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_ncol",     15, &ncol))     ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_infrom",   17, &infrom))   ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_into",     15, &into))     ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_readrg",   17, &readrg))   ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_ngroup",   17, &ngroup))   ) any_rc = rc;
    if ( (rc = sf_scalar_dbl("__sparquet_progress", 19, &progress)) ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_check",    16, &tevery))   ) any_rc = rc;

    // You don't adjust into in this case because we can loop from the
    // start, so no while ... trick
    --infrom;

    tobs   = into - infrom + 1;
    ttot   = ncol * tobs;
    tread  = 0;

    _readrg = readrg? readrg: 1;
    int64_t vtypes[ncol];
    int64_t _colix[ncol];
    int64_t _rowgix[_readrg];
    std::vector<int> colix(ncol);
    std::vector<int> rowgix(_readrg);

    // Adjust row group selection to be 0-indexed
    if ( (rc = sf_matrix_int("__sparquet_rowgix", 17, _readrg, _rowgix)) ) any_rc = rc;
    for (j = 0; j < _readrg; j++) {
        rowgix[j] = --_rowgix[j];
    }

    // Adjust column selection to be 0-indexed
    if ( (rc = sf_matrix_int("__sparquet_colix", 16, ncol, _colix)) ) any_rc = rc;
    for (j = 0; j < ncol; j++)
        colix[j] = --_colix[j];

    if ( any_rc ) {
        rc = any_rc;
        goto exit;
    }

    try {

        // Read entire table
        // -----------------

        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(
            fname, arrow::default_memory_pool(), &infile));

        std::unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

        if ( nthreads > 1 ) {

            // TODO version waarning: set_num_threads is deprecated and
            // I should only use set_use_threads in later versions.

            // reader->set_num_threads((int64_t) nthreads);
            reader->set_use_threads(true);
        }

        std::vector<std::shared_ptr<arrow::Table>> tables(_readrg, nullptr);
        if ( readrg ) {
            for (j = 0; j < _readrg; ++j) {
                PARQUET_THROW_NOT_OK(reader->ReadRowGroup({rowgix[j]}, colix, &tables[j]));
            }
        }
        else {
            PARQUET_THROW_NOT_OK(reader->ReadTable(colix, &tables[0]));
        }

        sf_running_timer (&timer, "Read data into Arrow table"); 
        ncol = tables[0]->num_columns();
        for (j = 1; j < _readrg; ++j) {
            if ( ncol != tables[j]->num_columns() ) {
                sf_errprintf("Inconsistent columns across row groups\n");
                rc = 17302;
                goto exit;
            }
        }

        // Parse types
        // -----------

        if ( (rc = sf_matrix_int("__sparquet_coltypes", 19, ncol, vtypes)) ) any_rc = rc;
        for (j = 0; j < ncol; j++)
            if ( vtypes[j] > maxstrlen ) maxstrlen = vtypes[j];

        SPARQUET_CHAR (vstr, maxstrlen);

        // Copy to stata
        // -------------

        parquet::Type::type id;
        std::shared_ptr<arrow::BooleanArray>         boolarray;
        std::shared_ptr<arrow::Int32Array>           i32array;
        std::shared_ptr<arrow::Int64Array>           i64array;
        std::shared_ptr<arrow::FloatArray>           floatarray;
        std::shared_ptr<arrow::DoubleArray>          doublearray;
        std::shared_ptr<arrow::StringArray>          strarray;
        std::shared_ptr<arrow::FixedSizeBinaryArray> flstrarray;

        ir = ic = ix = ig = 0;
        for (r = 0; r < _readrg; ++r) {
            ig = 0;
            for (j = 0; j < ncol; j++) {
                // vtype = vtypes[j];

                // TODO version warning: table->column()->data() maybe got
                // deprecated? just do table->column() in later versions.

                auto data = tables[r]->column(j);
                // auto data = tables[r]->column(j)->data();
                nchunks = data->num_chunks();
                ix = 0;
                ic = 0;
                id = get_physical_type(data->type()->id());
                for (c = 0; c < nchunks; c++) {
                    if ( id == Type::BOOLEAN )   {
                        boolarray = std::static_pointer_cast<arrow::BooleanArray>(data->chunk(c));
                        nfields = boolarray->num_fields();
                        if ( nfields > 1 ) {
                            sf_errprintf("Multiple fields not supported\n");
                            rc = 17042;
                            goto exit;
                        }
                        narrlen  = boolarray->length();
                        narrfrom = 0;
                        if ( narrlen > (into - (ir + ic)) ) {
                            narrlen = into - (ir + ic);
                        }
                        if ( infrom > (ir + ic) ) {
                            narrfrom = infrom - (ir + ic);
                        }
                        for (i = narrfrom; i < narrlen; i++) {
                            if ( (ix + nread) % tevery == 0 ) {
                                tread += tevery;
                                sf_running_progress_read(
                                    &timer,
                                    &stimer,
                                    progress,
                                    r + 1, ngroup,
                                    j + 1, ncol,
                                    ix + nread, tobs,
                                    100 * tread / ttot
                                );
                            }
                            if (boolarray->IsNull(i)) {
                                z = SV_missval;
                            }
                            else {
                                z = (double) boolarray->Value(i);
                            }
                            if ( (rc = SF_vstore(j + 1, ++ix + nread, z)) ) goto exit;
                        }
                        ic += boolarray->length();
                    }
                    else if ( id == Type::INT32 ) {
                        i32array = std::static_pointer_cast<arrow::Int32Array>(data->chunk(c));
                        nfields = i32array->num_fields();
                        if ( nfields > 1 ) {
                            sf_errprintf("Multiple fields not supported\n");
                            rc = 17042;
                            goto exit;
                        }
                        narrlen  = i32array->length();
                        narrfrom = 0;
                        if ( narrlen > (into - (ir + ic)) ) {
                            narrlen = into - (ir + ic);
                        }
                        if ( infrom > (ir + ic) ) {
                            narrfrom = infrom - (ir + ic);
                        }
                        for (i = narrfrom; i < narrlen; i++) {
                            if ( (ix + nread) % tevery == 0 ) {
                                tread += tevery;
                                sf_running_progress_read(
                                    &timer,
                                    &stimer,
                                    progress,
                                    r + 1, ngroup,
                                    j + 1, ncol,
                                    ix + nread, tobs,
                                    100 * tread / ttot
                                );
                            }
                            if (i32array->IsNull(i)) {
                                z = SV_missval;
                            }
                            else {
                                z = (double) i32array->Value(i);
                            }
                            if ( (rc = SF_vstore(j + 1, ++ix + nread, z)) ) goto exit;
                        }
                        ic += i32array->length();
                    }
                    else if ( id == Type::INT64 )  {
                        i64array = std::static_pointer_cast<arrow::Int64Array>(data->chunk(c));
                        nfields = i64array->num_fields();
                        if ( nfields > 1 ) {
                            sf_errprintf("Multiple fields not supported\n");
                            rc = 17042;
                            goto exit;
                        }
                        narrlen  = i64array->length();
                        narrfrom = 0;
                        if ( narrlen > (into - (ir + ic)) ) {
                            narrlen = into - (ir + ic);
                        }
                        if ( infrom > (ir + ic) ) {
                            narrfrom = infrom - (ir + ic);
                        }
                        for (i = narrfrom; i < narrlen; i++) {
                            if ( (ix + nread) % tevery == 0 ) {
                                tread += tevery;
                                sf_running_progress_read(
                                    &timer,
                                    &stimer,
                                    progress,
                                    r + 1, ngroup,
                                    j + 1, ncol,
                                    ix + nread, tobs,
                                    100 * tread / ttot
                                );
                            }
                            if (i64array->IsNull(i)) {
                                z = SV_missval;
                            }
                            else {
                                z = (double) i64array->Value(i);
                            }
                            if ( (rc = SF_vstore(j + 1, ++ix + nread, z)) ) goto exit;
                        }
                        ic += i64array->length();
                    }
                    else if ( id == Type::INT96 ) {
                        sf_errprintf("96-bit integers not implemented.\n");
                        rc = 17101;
                        goto exit;
                    }
                    else if ( id == Type::FLOAT ) {
                        floatarray = std::static_pointer_cast<arrow::FloatArray>(data->chunk(c));
                        nfields = floatarray->num_fields();
                        if ( nfields > 1 ) {
                            sf_errprintf("Multiple fields not supported\n");
                            rc = 17042;
                            goto exit;
                        }
                        narrlen  = floatarray->length();
                        narrfrom = 0;
                        if ( narrlen > (into - (ir + ic)) ) {
                            narrlen = into - (ir + ic);
                        }
                        if ( infrom > (ir + ic) ) {
                            narrfrom = infrom - (ir + ic);
                        }
                        for (i = narrfrom; i < narrlen; i++) {
                            if ( (ix + nread) % tevery == 0 ) {
                                tread += tevery;
                                sf_running_progress_read(
                                    &timer,
                                    &stimer,
                                    progress,
                                    r + 1, ngroup,
                                    j + 1, ncol,
                                    ix + nread, tobs,
                                    100 * tread / ttot
                                );
                            }
                            if (floatarray->IsNull(i)) {
                                z = SV_missval;
                            }
                            else {
                                z = (double) floatarray->Value(i);
                            }
                            if ( (rc = SF_vstore(j + 1, ++ix + nread, z)) ) goto exit;
                        }
                        ic += floatarray->length();
                    }
                    else if ( id == Type::DOUBLE )     {
                        doublearray = std::static_pointer_cast<arrow::DoubleArray>(data->chunk(c));
                        nfields = doublearray->num_fields();
                        if ( nfields > 1 ) {
                            sf_errprintf("Multiple fields not supported\n");
                            rc = 17042;
                            goto exit;
                        }
                        narrlen  = doublearray->length();
                        narrfrom = 0;
                        if ( narrlen > (into - (ir + ic)) ) {
                            narrlen = into - (ir + ic);
                        }
                        if ( infrom > (ir + ic) ) {
                            narrfrom = infrom - (ir + ic);
                        }
                        for (i = narrfrom; i < narrlen; i++) {
                            if ( (ix + nread) % tevery == 0 ) {
                                tread += tevery;
                                sf_running_progress_read(
                                    &timer,
                                    &stimer,
                                    progress,
                                    r + 1, ngroup,
                                    j + 1, ncol,
                                    ix + nread, tobs,
                                    100 * tread / ttot
                                );
                            }
                            if (doublearray->IsNull(i)) {
                                z = SV_missval;
                            }
                            else {
                                z = doublearray->Value(i);
                            }
                            if ( (rc = SF_vstore(j + 1, ++ix + nread, z)) ) goto exit;
                        }
                        ic += doublearray->length();
                    }
                    else if ( id == Type::BYTE_ARRAY ) {
                        strarray = std::static_pointer_cast<arrow::StringArray>(data->chunk(c));
                        nfields = strarray->num_fields();
                        if ( nfields > 1 ) {
                            sf_errprintf("Multiple fields not supported\n");
                            rc = 17042;
                            goto exit;
                        }
                        narrlen  = strarray->length();
                        narrfrom = 0;
                        if ( narrlen > (into - (ir + ic)) ) {
                            narrlen = into - (ir + ic);
                        }
                        if ( infrom > (ir + ic) ) {
                            narrfrom = infrom - (ir + ic);
                        }
                        // TODO: Check GetString won't fail w/actyally binary data
                        for (i = narrfrom; i < narrlen; i++) {
                            if ( (ix + nread) % tevery == 0 ) {
                                tread += tevery;
                                sf_running_progress_read(
                                    &timer,
                                    &stimer,
                                    progress,
                                    r + 1, ngroup,
                                    j + 1, ncol,
                                    ix + nread, tobs,
                                    100 * tread / ttot
                                );
                            }
                            // memcpy(vstr, strarray->GetString(i), strarray->value_length(i));
                            if ( strarray->value_length(i) > vtypes[j] ) {
                                sf_errprintf("Buffer (%d) too small; re-run with larger buffer or -strscan(.)-\n",
                                             vtypes[j]);
                                sf_errprintf("Chunk %d, row %d, col %d had a string of length %d.\n",
                                             c, i + ix + 1, j, strarray->value_length(i));
                                rc = 17103;
                                goto exit;
                            }
                            strarray->GetString(i).copy(vstr, vtypes[j]);
                            if ( (rc = SF_sstore(j + 1, ++ix + nread, vstr)) ) goto exit;
                            memset(vstr, '\0', maxstrlen);
                        }
                        ic += strarray->length();
                    }
                    else if ( id == Type::FIXED_LEN_BYTE_ARRAY ) {
                        flstrarray = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(data->chunk(c));
                        nfields = flstrarray->num_fields();
                        if ( nfields > 1 ) {
                            sf_errprintf("Multiple fields not supported\n");
                            rc = 17042;
                            goto exit;
                        }
                        narrlen  = flstrarray->length();
                        narrfrom = 0;
                        if ( narrlen > (into - (ir + ic)) ) {
                            narrlen = into - (ir + ic);
                        }
                        if ( infrom > (ir + ic) ) {
                            narrfrom = infrom - (ir + ic);
                        }
                        // TODO: Check this actually works?
                        // TODO: GetString won't fail w/actyally binary data
                        for (i = narrfrom; i < narrlen; i++) {
                            if ( (ix + nread) % tevery == 0 ) {
                                tread += tevery;
                                sf_running_progress_read(
                                    &timer,
                                    &stimer,
                                    progress,
                                    r + 1, ngroup,
                                    j + 1, ncol,
                                    ix + nread, tobs,
                                    100 * tread / ttot
                                );
                            }
                            memcpy(vstr, flstrarray->GetValue(i), flstrarray->byte_width());
                            // memcpy(vstr, flstrarray->GetValue(i), vtype);
                            if ( (rc = SF_sstore(j + 1, ++ix + nread, vstr)) ) goto exit;
                            memset(vstr, '\0', maxstrlen);
                        }
                        ic += flstrarray->length();
                    }
                    else {
                        sf_errprintf("Unknown parquet type.\n");
                        rc = 17100;
                        goto exit;
                    }
                }
                ig = ix > ig? ix: ig;
            }
            nread += ig;
            ir += tables[r]->num_rows();
        }

        sf_running_timer (&timer, "Copied table into Stata");

        sf_printf_debug(verbose, "\tFile:    %s\n",  fname);
        sf_printf_debug(verbose, "\tColumns: %ld\n", ncol);
        sf_printf_debug(verbose, "\tRows:    %ld\n", into - infrom);

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

    if ( (into - infrom) != nread ) {
        sf_errprintf("Warning: Expected %ld obs but found %ld\n",
                     (into - infrom), nread);
    }

    memcpy(vscalar, "__sparquet_nread", 16);
    if ( (rc = SF_scal_save(vscalar, (ST_double) nread)) ) goto exit;

exit:
  return(rc);
}
