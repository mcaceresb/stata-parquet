clear
set obs 10
gen byte   byte1    = _n * 1
gen int    int1     = _n * 8
gen long   long1    = _n * 42
gen float  float1   = runiform()
gen double double1  = rnormal()
gen str32  string32 = "something here"
desc
l
parquet save test-stata.parquet, replace

parquet use test-stata.parquet, clear
compress
desc
l
parquet save using test-stata2.parquet, replace

* ./build.py --test && tail build/parquet_tests.log
* export LD_LIBRARY_PATH=/usr/local/lib64
* import pandas as pd
* df = pd.read_parquet('test-stata.parquet')
