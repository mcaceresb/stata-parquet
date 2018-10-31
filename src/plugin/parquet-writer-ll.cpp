// Stata function: Low-level write full varlist
// __sparquet_ncol must exist and be a numeric scalar with the # of cols
// __sparquet_coltypes must exist and be a 1 by # col matrix
// __sparquet_fixedlen must exist and be a 1 by # col matrix
ST_retcode sf_ll_write_varlist(
    const char *fname,
    const char *fcols,
    const int verbose,
    const int debug,
    const int strbuffer)
{

    ST_double z;
    ST_retcode rc = 0, any_rc = 0;
    SPARQUET_CHAR(vmatrix, 32);
    SPARQUET_CHAR(vscalar, 32);
    SPARQUET_CHAR(vstr, strbuffer);


    int16_t definition_level = 1;
    int64_t in1 = SF_in1();
    int64_t in2 = SF_in2();
    int64_t N = in2 - in1 + 1;
    int64_t vtype, i, j, ncol = 1, fixedlen = 0;

    std::string line;
    std::ifstream fstream;

    bool vbool;
    int32_t vint32;
    float vfloat;
    double vdouble;
    parquet::ByteArray vbytearray;
    parquet::FixedLenByteArray vfixedlen;

    parquet::BoolWriter* bool_writer;
    parquet::Int32Writer* int32_writer;
    parquet::FloatWriter* float_writer;
    parquet::DoubleWriter* double_writer;
    parquet::ByteArrayWriter* ba_writer;
    parquet::FixedLenByteArrayWriter* flba_writer;

    // Get column and type info from Stata
    // -----------------------------------

    memset(vscalar, '\0', 32);
    memcpy(vscalar, "__sparquet_fixedlen", 19);
    if ( (rc = SF_scal_use(vscalar, &z)) ) {
        any_rc = rc;
    }
    else {
        fixedlen = (int64_t) z;
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

    if ( any_rc ) goto exit;

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
        using FileClass = ::arrow::io::FileOutputStream;
        std::shared_ptr<FileClass> out_file;
        std::shared_ptr<GroupNode> schema;
        std::shared_ptr<parquet::WriterProperties> props;
        std::shared_ptr<parquet::ParquetFileWriter> file_writer;
        parquet::WriterProperties::Builder builder;
        parquet::schema::NodeVector fields;

        // Get fields
        // ----------

        // TODO: Logical type is basically format?
        for (j = 0; j < ncol; j++) {
            vtype = vtypes[j];
            if ( vtype == -1 ) {
                fields.push_back(
                    PrimitiveNode::Make(
                        vnames[j].c_str(),
                        Repetition::REQUIRED,
                        Type::BOOLEAN,
                        LogicalType::NONE
                    )
                );
            }
            else if ( vtype == -2 ) {
                fields.push_back(
                    PrimitiveNode::Make(
                        vnames[j].c_str(),
                        Repetition::REQUIRED,
                        Type::INT32,
                        LogicalType::NONE
                    )
                );
            }
            else if ( vtype == -3 ) {
                fields.push_back(
                    PrimitiveNode::Make(
                        vnames[j].c_str(),
                        Repetition::REQUIRED,
                        Type::INT32,
                        LogicalType::NONE
                    )
                );
            }
            else if ( vtype == -4 ) {
                fields.push_back(
                    PrimitiveNode::Make(
                        vnames[j].c_str(),
                        Repetition::REQUIRED,
                        Type::FLOAT,
                        LogicalType::NONE
                    )
                );
            }
            else if ( vtype == -5 ) {
                fields.push_back(
                    PrimitiveNode::Make(
                        vnames[j].c_str(),
                        Repetition::REQUIRED,
                        Type::DOUBLE,
                        LogicalType::NONE
                    )
                );
            }
            else if ( vtype > 0 ) {
                if ( fixedlen ) {
                    fields.push_back(
                        PrimitiveNode::Make(
                            vnames[j].c_str(),
                            Repetition::REQUIRED,
                            Type::FIXED_LEN_BYTE_ARRAY,
                            LogicalType::NONE,
                            vtype
                        )
                    );
                }
                else {
                    fields.push_back(
                        PrimitiveNode::Make(
                            vnames[j].c_str(),
                            Repetition::OPTIONAL,
                            Type::BYTE_ARRAY,
                            LogicalType::NONE
                        )
                    );
                }
            }
            else {
                sf_errprintf("Unsupported type.\n");
                rc = 17100;
                goto exit;
            }
        }

        // Write to file
        // -------------

        PARQUET_THROW_NOT_OK(FileClass::Open(fname, &out_file));
        schema = std::static_pointer_cast<GroupNode>(
            GroupNode::Make("schema", Repetition::REQUIRED, fields)
        );

        builder.compression(parquet::Compression::SNAPPY);
        props = builder.build();

        file_writer = parquet::ParquetFileWriter::Open(out_file, schema, props);
        parquet::RowGroupWriter* rg_writer = file_writer->AppendRowGroup(N);

        clock_t timer = clock();
        for (j = 0; j < ncol; j++) {
            vtype = vtypes[j];
            // TODO: Boolean is only 0/1; keep? Or always int32?
            // TODO: Boolean does NOT support missing values; leaning toward dropping
            // TODO: Missing values
            if ( vtype == -1 ) {
                bool_writer = static_cast<parquet::BoolWriter*>(rg_writer->NextColumn());
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    // sf_printf_debug(2, "\t(bool, %ld, %ld): %9.4f\n", j, i, z);
                    vbool = (bool) z;
                    bool_writer->WriteBatch(1, nullptr, nullptr, &vbool);
                }
            }
            else if ( vtype == -2 ) {
                int32_writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    // sf_printf_debug(2, "\t(int, %ld, %ld): %9.4f\n", j, i, z);
                    vint32 = (int32_t) z;
                    int32_writer->WriteBatch(1, nullptr, nullptr, &vint32);
                }
            }
            else if ( vtype == -3 ) {
                int32_writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    // sf_printf_debug(2, "\t(long, %ld, %ld): %9.4f\n", j, i, z);
                    vint32 = (int32_t) z;
                    int32_writer->WriteBatch(1, nullptr, nullptr, &vint32);
                }
            }
            else if ( vtype == -4 ) {
                float_writer = static_cast<parquet::FloatWriter*>(rg_writer->NextColumn());
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    // sf_printf_debug(2, "\t(float, %ld, %ld): %9.4f\n", j, i, z);
                    vfloat = (float) z;
                    float_writer->WriteBatch(1, nullptr, nullptr, &vfloat);
                }
            }
            else if ( vtype == -5 ) {
                double_writer = static_cast<parquet::DoubleWriter*>(rg_writer->NextColumn());
                for (i = 0; i < N; i++) {
                    if ( (rc = SF_vdata(j + 1, i + in1, &z)) ) goto exit;
                    // sf_printf_debug(2, "\t(double, %ld, %ld): %9.4f\n", j, i, z);
                    vdouble = (double) z;
                    double_writer->WriteBatch(1, nullptr, nullptr, &vdouble);
                }
            }
            else if ( vtype > 0 ) {
                // TODO: At the moment, fixed-length are padded with " "
                if ( fixedlen ) {
                    flba_writer = static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->NextColumn());
                    for (i = 0; i < N; i++) {
                        // memset(vstr, ' ', vtype);
                        if ( (rc = SF_sdata(j + 1, i + in1, vstr)) ) goto exit;
                        // sf_printf_debug(2, "\t(FWBA, %ld, %ld): %s\n", j, i, vstr);
                        vfixedlen.ptr = reinterpret_cast<const uint8_t*>(&vstr[0]);
                        flba_writer->WriteBatch(1, nullptr, nullptr, &vfixedlen);
                        memset(vstr, '\0', strbuffer);
                    }
                }
                else {
                    ba_writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
                    for (i = 0; i < N; i++) {
                        if ( (rc = SF_sdata(j + 1, i + in1, vstr)) ) goto exit;
                        // sf_printf_debug(2, "\t(BA, %ld, %ld): %s\n", j, i, vstr);
                        vbytearray.ptr = reinterpret_cast<const uint8_t*>(&vstr[0]);
                        vbytearray.len = strlen(vstr);
                        ba_writer->WriteBatch(1, &definition_level, nullptr, &vbytearray);
                        memset(vstr, '\0', strbuffer);
                    }
                }
            }
            else {
                sf_errprintf("Unsupported type.\n");
                rc = 17100;
                goto exit;
            }
        }
        sf_running_timer (&timer, "Wrote data from memory to disk");
    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
    return (rc);
}
