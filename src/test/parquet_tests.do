disp "Hello from Stata"

* Some test data
* --------------

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
parquet write using test-stata.parquet, replace

parquet read using test-stata.parquet, clear
compress
desc
l
parquet write using test-stata2.parquet, replace
