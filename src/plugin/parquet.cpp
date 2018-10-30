#include "spi/stplugin.h"
#include <string.h>
#include <string>

#include <cassert>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>

#include "reader_writer.h"
#include "parquet.h"

constexpr int NUM_ROWS_PER_ROW_GROUP = 500;
// const char PARQUET_FILENAME[] = "test.parquet";
const char PARQUET_FILENAME[] = "test-python.parquet";
// const char PARQUET_FILENAME[] = "test-stata.parquet";

STDLL stata_call(int argc, char *argv[])
{
    ST_retcode rc = 0;
    ST_double z;
    sf_printf_debug("Hello from C++\n");
    int64_t maxstrlen = 8;
    char *vstr = new char[maxstrlen];
    memset(vstr, '\0', maxstrlen);

    // int64_t values_read;
    // int64_t rows_read;
    int64_t r, i, j;

    // Declare all the values;
    // bool vbool;
    // int32_t vint32;
    // int64_t vint64;
    // parquet::Int96 vint96;
    // float vfloat;
    // double vdouble;
    // parquet::ByteArray vbytearray;
    // parquet::FixedLenByteArray vfixedlen;

exit:
    sf_printf_debug("Goodbye from C++\n");
    return (rc) ;
}

//./build.py --test && tail build/parquet_tests.log
// LD_LIBRARY_PATH=/usr/local/lib64
// import pandas as pd
// df = pd.read_parquet('test.parquet', engine='pyarrow')
