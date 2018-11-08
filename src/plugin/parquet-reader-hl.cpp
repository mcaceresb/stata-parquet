// Stata function: High-level read full varlist
//
// matrices
//     __sparquet_coltypes
//     __sparquet_colix
// scalars
//     __sparquet_ncol
//     __sparquet_rg_size
//     __sparquet_threads
//     __sparquet_into
//     __sparquet_infrom

ST_retcode sf_hl_read_varlist(
    const char *fname,
    const int verbose,
    const int debug,
    const uint64_t strbuffer)
{
    ST_double z;
    ST_retcode rc = 0, any_rc = 0;
    clock_t timer = clock();
    int64_t i, j, c, ix;
    int64_t nfields, narrfrom, narrlen, nchunks;
    int64_t maxstrlen = 1, nthreads = 1, ncol = 1, infrom = 1, into = 1;

    // int64_t vtype;
    SPARQUET_CHAR(vmatrix, 32);

    if ( (rc = sf_scalar_int("__sparquet_threads", 18, &nthreads)) ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_ncol",    15, &ncol))     ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_infrom",  17, &infrom))   ) any_rc = rc;
    if ( (rc = sf_scalar_int("__sparquet_into",    15, &into))     ) any_rc = rc;

    // You don't adjust into in this case because we can loop from the
    // start, so no while ... trick
    --infrom;

    int64_t vtypes[ncol];
    int64_t _colix[ncol];
    std::vector<int> colix(ncol);

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
            // TODO: set_num_threads is deprecated and I should only use
            // set_use_threads? If so, change option to be binary.
            // reader->set_num_threads((int64_t) nthreads);
            reader->set_use_threads(true);
        }
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(colix, &table));

        sf_running_timer (&timer, "Read data into Arrow table");

        ncol = table->num_columns();

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

        for (j = 0; j < ncol; j++) {
            ix = 1;
            // vtype = vtypes[j];
            auto data = table->column(j)->data();
            nchunks = data->num_chunks();
            for (c = 0; c < nchunks; c++) {
                id = get_physical_type(data->type()->id());
                if ( id == Type::BOOLEAN )   {
                    boolarray = std::static_pointer_cast<arrow::BooleanArray>(data->chunk(c));
                    nfields = boolarray->num_fields();
                    if ( nfields > 1 ) {
                        sf_errprintf("Multiple fields not supported\n");
                        rc = 17042;
                        goto exit;
                    }
                    narrlen = boolarray->length();
                    if ( narrlen > into ) narrlen = into;
                    narrfrom = (infrom + ix - 1);
                    for (i = narrfrom; i < narrlen; i++) {
                        if (boolarray->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = (double) boolarray->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix - infrom, z)) ) goto exit;
                    }
                }
                else if ( id == Type::INT32 ) {
                    i32array = std::static_pointer_cast<arrow::Int32Array>(data->chunk(c));
                    nfields = i32array->num_fields();
                    if ( nfields > 1 ) {
                        sf_errprintf("Multiple fields not supported\n");
                        rc = 17042;
                        goto exit;
                    }
                    narrlen = i32array->length();
                    if ( narrlen > into ) narrlen = into;
                    narrfrom = (infrom + ix - 1);
                    for (i = narrfrom; i < narrlen; i++) {
                        if (i32array->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = (double) i32array->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix - infrom, z)) ) goto exit;
                    }
                }
                else if ( id == Type::INT64 )  {
                    i64array = std::static_pointer_cast<arrow::Int64Array>(data->chunk(c));
                    nfields = i64array->num_fields();
                    if ( nfields > 1 ) {
                        sf_errprintf("Multiple fields not supported\n");
                        rc = 17042;
                        goto exit;
                    }
                    narrlen = i64array->length();
                    if ( narrlen > into ) narrlen = into;
                    narrfrom = (infrom + ix - 1);
                    for (i = narrfrom; i < narrlen; i++) {
                        if (i64array->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = (double) i64array->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix - infrom, z)) ) goto exit;
                    }
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
                    narrlen = floatarray->length();
                    if ( narrlen > into ) narrlen = into;
                    narrfrom = (infrom + ix - 1);
                    for (i = narrfrom; i < narrlen; i++) {
                        if (floatarray->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = (double) floatarray->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix - infrom, z)) ) goto exit;
                    }
                }
                else if ( id == Type::DOUBLE )     {
                    doublearray = std::static_pointer_cast<arrow::DoubleArray>(data->chunk(c));
                    nfields = doublearray->num_fields();
                    if ( nfields > 1 ) {
                        sf_errprintf("Multiple fields not supported\n");
                        rc = 17042;
                        goto exit;
                    }
                    narrlen = doublearray->length();
                    if ( narrlen > into ) narrlen = into;
                    narrfrom = (infrom + ix - 1);
                    for (i = narrfrom; i < narrlen; i++) {
                        if (doublearray->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = doublearray->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix - infrom, z)) ) goto exit;
                    }
                }
                else if ( id == Type::BYTE_ARRAY ) {
                    strarray = std::static_pointer_cast<arrow::StringArray>(data->chunk(c));
                    nfields = strarray->num_fields();
                    if ( nfields > 1 ) {
                        sf_errprintf("Multiple fields not supported\n");
                        rc = 17042;
                        goto exit;
                    }
                    narrlen = strarray->length();
                    if ( narrlen > into ) narrlen = into;
                    narrfrom = (infrom + ix - 1);
                    // TODO: Check GetString won't fail w/actyally binary data
                    for (i = narrfrom; i < narrlen; i++) {
                        // memcpy(vstr, strarray->GetString(i), strarray->value_length(i));
                        if ( strarray->value_length(i) > vtypes[j] ) {
                            sf_errprintf("Buffer (%d) too small; re-run with larger buffer or -strscan(.)-\n", vtypes[j]);
                            sf_errprintf("Chunk %d, row %d, col %d had a string of length %d.\n", c, i + ix, j, strarray->value_length(i));
                            rc = 17103;
                            goto exit;
                        }
                        strarray->GetString(i).copy(vstr, vtypes[j]);
                        if ( (rc = SF_sstore(j + 1, i + ix - infrom, vstr)) ) goto exit;
                        memset(vstr, '\0', maxstrlen);
                    }
                }
                else if ( id == Type::FIXED_LEN_BYTE_ARRAY ) {
                    flstrarray = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(data->chunk(c));
                    nfields = flstrarray->num_fields();
                    if ( nfields > 1 ) {
                        sf_errprintf("Multiple fields not supported\n");
                        rc = 17042;
                        goto exit;
                    }
                    narrlen = flstrarray->length();
                    if ( narrlen > into ) narrlen = into;
                    narrfrom = (infrom + ix - 1);
                    // TODO: Check this actually works?
                    // TODO: GetString won't fail w/actyally binary data
                    for (i = narrfrom; i < narrlen; i++) {
                        memcpy(vstr, flstrarray->GetValue(i), flstrarray->byte_width());
                        // memcpy(vstr, flstrarray->GetValue(i), vtype);
                        if ( (rc = SF_sstore(j + 1, i + ix - infrom, vstr)) ) goto exit;
                        memset(vstr, '\0', maxstrlen);
                    }
                }
                else {
                    sf_errprintf("Unknown parquet type.\n");
                    rc = 17100;
                    goto exit;
                }
                ix += i;
            }
        }

        sf_running_timer (&timer, "Copied table into Stata");

        sf_printf_debug(verbose, "\tFile:    %s\n",  fname);
        sf_printf_debug(verbose, "\tColumns: %ld\n", ncol);
        sf_printf_debug(verbose, "\tRows:    %ld\n", into - infrom + 1);

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
  return(rc);
}
