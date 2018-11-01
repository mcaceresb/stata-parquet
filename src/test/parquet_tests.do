* README
* ------

sysuse auto, clear
parquet save auto.parquet, replace
parquet use auto.parquet, clear
desc

parquet use price make gear_ratio using auto.parquet, clear
desc
parquet save gear_ratio make using auto.parquet, replace

sysuse auto, clear
parquet save auto.parquet, replace lowlevel
parquet use auto.parquet, clear lowlevel
desc

parquet use price make gear_ratio using auto.parquet, clear lowlevel
desc
parquet save gear_ratio make using auto.parquet, replace lowlevel

* Benchmarks
* ----------

set rmsg on
clear
set obs 5000000
gen float  x1 = runiform()
gen double x2 = rnormal()
gen long   l1 = 100 * int(runiform())
gen str7   s7 = "hello"
parquet save x2 using tmp.parquet, replace
parquet save tmp.parquet, replace
save tmp, replace
* export delimited using "tmp.csv", replace

if ( `c(MP)' ) parquet use tmp.parquet, clear threads(4)
parquet use x2 using tmp.parquet, clear
parquet use tmp.parquet, clear
use tmp.dta, clear
* import delimited using "tmp.csv", clear varn(1)
set rmsg off

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
parquet save test-stata.parquet, replace

parquet use test-stata.parquet, clear lowlevel
desc
compress
l
parquet save using test-stata2.parquet, replace lowlevel fixedlen
parquet use test-stata.parquet, clear
desc

* read/write
* read/write lowlevel
* read/write varlist using in range
* asserts galore!
