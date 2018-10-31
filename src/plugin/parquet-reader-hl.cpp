// Stata function: High-level read full varlist
// __sparquet_ncol must exist and be a numeric scalar with the # of cols
// __sparquet_coltypes must exist and be a 1 by # col matrix
// __sparquet_rg_size must exist and be a 1 by # col matrix
ST_retcode sf_hl_read_varlist(
    const char *fname,
    const int verbose,
    const int debug,
    const uint64_t strbuffer)
{
    ST_retcode rc = 0;
    clock_t timer = clock();
    int64_t nrow, ncol, i, j;

    try {
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(
            fname, arrow::default_memory_pool(), &infile));

        std::unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

        // reader->set_use_threads(true);
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

        // std::shared_ptr<arrow::Array> array;
        // PARQUET_THROW_NOT_OK(reader->ReadColumn(0, &array));

        sf_running_timer (&timer, "Read data from disk into Arrow table");

        ncol = table->num_columns();
        nrow = table->num_rows();

        sf_printf_debug(verbose, "\tFile:    %s\n",  fname);
        sf_printf_debug(verbose, "\tColumns: %ld\n", ncol);
        sf_printf_debug(verbose, "\tRows:    %ld\n", nrow);

        for (i = 0; i < nrow; i++) {
            for (j = 0; j < ncol; j++) {
                // sf_printf_debug(debug, "\t(%ld, %ld): %9.4f\n", j, i,
                //         (ST_double) table->column(j)->data()->chunk(i)->raw_values());
                // table->column(j)->data()->chunk(1);
                // switch (table->schema()->field(j)->type()) {
                //     case Type::BOOLEAN:    // byte
                //         break;
                //     case Type::INT32:      // long
                //         break;
                //     case Type::INT64:      // double
                //         break;
                //     case Type::INT96:
                //         break;
                //     case Type::DOUBLE:     // double
                //         break;
                //     case Type::BYTE_ARRAY: // str#, strL
                //         break;
                //     case Type::FIXED_LEN_BYTE_ARRAY:
                //         break;
                //     default:
                // }
            }
        }

        sf_running_timer (&timer, "Copied data from arrow table into Stata");

    } catch (const std::exception& e) {
        sf_errprintf("Parquet read error: %s\n", e.what());
        return(-1);
    }

exit:
  return(rc);
}

// void read_single_column() {
//   std::cout << "Reading first column of parquet-arrow-example.parquet" << std::endl;
//   std::shared_ptr<arrow::io::ReadableFile> infile;
//   PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(
//       "parquet-arrow-example.parquet", arrow::default_memory_pool(), &infile));
//
//   std::unique_ptr<parquet::arrow::FileReader> reader;
//   PARQUET_THROW_NOT_OK(
//       parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
//   std::shared_ptr<arrow::Array> array;
//   PARQUET_THROW_NOT_OK(reader->ReadColumn(0, &array));
//   PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
//   std::cout << std::endl;
// }
//
// // #5: Read only a single column of a RowGroup (this is known as ColumnChunk)
// //     from the Parquet file.
// void read_single_column_chunk() {
//   std::cout << "Reading first ColumnChunk of the first RowGroup of "
//                "parquet-arrow-example.parquet"
//             << std::endl;
//   std::shared_ptr<arrow::io::ReadableFile> infile;
//   PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(
//       "parquet-arrow-example.parquet", arrow::default_memory_pool(), &infile));
//
//   std::unique_ptr<parquet::arrow::FileReader> reader;
//   PARQUET_THROW_NOT_OK(
//       parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
//   std::shared_ptr<arrow::Array> array;
//   PARQUET_THROW_NOT_OK(reader->RowGroup(0)->Column(0)->Read(&array));
//   PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
//   std::cout << std::endl;
// }
