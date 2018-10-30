// try {
//     int64_t in1 = SF_in1();
//     int64_t in2 = SF_in2();
//     int64_t N   = in2 - in1 + 1;
//     int16_t definition_level = 1;
//
//     arrow::Int32Builder i32builder;
//         for (i = 0; i < N; i++) {
//             if ( (rc = SF_vdata(1, i + in1, &z)) ) goto exit;
//             PARQUET_THROW_NOT_OK(i32builder.Append((int32_t) z));
//         }
//         std::shared_ptr<arrow::Array> i32array;
//         PARQUET_THROW_NOT_OK(i32builder.Finish(&i32array));
//
//     arrow::FloatBuilder f32builder;
//         for (i = 0; i < N; i++) {
//             if ( (rc = SF_vdata(2, i + in1, &z)) ) goto exit;
//             PARQUET_THROW_NOT_OK(f32builder.Append((float) z));
//         }
//         std::shared_ptr<arrow::Array> f32array;
//         PARQUET_THROW_NOT_OK(f32builder.Finish(&f32array));
//
//     arrow::StringBuilder strbuilder;
//         for (i = 0; i < N; i++) {
//             if ( (rc = SF_sdata(3, i + in1, vstr)) ) goto exit;
//             PARQUET_THROW_NOT_OK(strbuilder.Append(vstr));
//             memset(vstr, '\0', maxstrlen);
//         }
//         std::shared_ptr<arrow::Array> strarray;
//         PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));
//
//     std::shared_ptr<arrow::Schema> schema = arrow::schema(
//             {arrow::field("int", arrow::int32()),
//             arrow::field("float", arrow::float32()),
//             arrow::field("str", arrow::utf8())});
//     std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {i32array, f32array, strarray});
//
//     std::shared_ptr<arrow::io::FileOutputStream> outfile;
//     PARQUET_THROW_NOT_OK(
//             arrow::io::FileOutputStream::Open(PARQUET_FILENAME, &outfile));
//     PARQUET_THROW_NOT_OK(
//             parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 3));
// } catch (const std::exception& e) {
//     std::cerr << "Parquet read error: " << e.what() << std::endl;
//     return -1;
// }
