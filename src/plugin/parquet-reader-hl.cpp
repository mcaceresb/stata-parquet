// Stata function: High-level read full varlist
// __sparquet_ncol must exist and be a numeric scalar with the # of cols
// __sparquet_coltypes must exist and be a 1 by # col matrix
// __sparquet_colix must exist and be a 1 by # col matrix
// __sparquet_rg_size must exist and be a scalar
// __sparquet_threads must exist and be a scalar
ST_retcode sf_hl_read_varlist(
    const char *fname,
    const int verbose,
    const int debug,
    const uint64_t strbuffer)
{
    ST_double z;
    ST_retcode rc = 0, any_rc = 0;
    clock_t timer = clock();
    int64_t i, j, c, ix, maxstrlen;
    int64_t nthreads, nfields, narrlen, nchunks, nrow, ncol;

    // int64_t vtype;
    SPARQUET_CHAR(vmatrix, 32);
    SPARQUET_CHAR(vscalar, 32);

    memcpy(vscalar, "__sparquet_threads", 18);
    if ( (rc = SF_scal_use(vscalar, &z)) ) {
        any_rc = rc;
        nthreads = 1;
    }
    else {
        nthreads = (int64_t) z;
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

    // Adjust column selection to be 0-indexed
    std::vector<int> colix(ncol);
    memcpy(vmatrix, "__sparquet_colix", 16);
    for (j = 0; j < ncol; j++) {
        if ( (rc = SF_mat_el(vmatrix, 1, j + 1, &z)) ) goto exit;
        colix[j] = (int) z - 1;
    }

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
        nrow = table->num_rows();

        // Parse types
        // -----------

        int64_t vtypes[ncol];

        maxstrlen = 1;
        memset(vmatrix, '\0', 32);
        memcpy(vmatrix, "__sparquet_coltypes", 19);
        for (j = 0; j < ncol; j++) {
            if ( (rc = SF_mat_el(vmatrix, 1, j + 1, &z)) ) goto exit;
            vtypes[j] = (int64_t) z;
            if ( vtypes[j] > maxstrlen ) maxstrlen = vtypes[j];
        }
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
                    for (i = 0; i < narrlen; i++) {
                        if (boolarray->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = (double) boolarray->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix, z)) ) goto exit;
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
                    for (i = 0; i < narrlen; i++) {
                        if (i32array->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = (double) i32array->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix, z)) ) goto exit;
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
                    for (i = 0; i < narrlen; i++) {
                        if (i64array->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = (double) i64array->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix, z)) ) goto exit;
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
                    for (i = 0; i < narrlen; i++) {
                        if (floatarray->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = (double) floatarray->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix, z)) ) goto exit;
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
                    for (i = 0; i < narrlen; i++) {
                        if (doublearray->IsNull(i)) {
                            z = SV_missval;
                        }
                        else {
                            z = doublearray->Value(i);
                        }
                        if ( (rc = SF_vstore(j + 1, i + ix, z)) ) goto exit;
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
                    // TODO: Check GetString won't fail w/actyally binary data
                    for (i = 0; i < narrlen; i++) {
                        // memcpy(vstr, strarray->GetString(i), strarray->value_length(i));
                        strarray->GetString(i).copy(vstr, vtypes[j]);
                        if ( (rc = SF_sstore(j + 1, i + ix, vstr)) ) goto exit;
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
                    // TODO: Check this actually works?
                    // TODO: GetString won't fail w/actyally binary data
                    for (i = 0; i < narrlen; i++) {
                        memcpy(vstr, flstrarray->GetValue(i), flstrarray->byte_width());
                        // memcpy(vstr, flstrarray->GetValue(i), vtype);
                        if ( (rc = SF_sstore(j + 1, i + ix, vstr)) ) goto exit;
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
        sf_printf_debug(verbose, "\tRows:    %ld\n", nrow);

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
  return(rc);
}
