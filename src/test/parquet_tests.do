* Benchmarks
* ----------

set rmsg on
clear
set obs 10000000
gen float  x1 = runiform()
gen double x2 = rnormal()
gen long   l1 = 100 * int(runiform())
parquet save tmp.parquet, replace
save tmp, replace
* export delimited using "tmp.csv", replace

use tmp.dta, clear
parquet read tmp.parquet, clear
* import delimited using "tmp.csv", clear varn(1)

* Basic
* -----

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
parquet save test-stata.parquet, replace lowlevel fixedlen

parquet use test-stata.parquet, clear
compress
desc
l
parquet save using test-stata2.parquet, replace
