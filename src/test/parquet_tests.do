* Hive
* ----

if ( `:list posof `"python"' in 0' == 0 ) {
    !printf "\nimport pandas as pd \nimport numpy as np \nimport fastparquet as fp \ndf = pd.DataFrame(np.random.randint(0,100,size=(8, 4)), columns=list('ABCD')) \ndf['tmp']= '#GiraffesAreFake' \nfp.write('test.parquet', df, row_group_offsets=2, file_scheme='hive')\nfp.write('testrg.parquet', df, row_group_offsets=2)" | python
    cap noi parquet use using test.parquet, clear rg(1 3 2) highlevel
    cap noi parquet use using test.parquet, clear rg(1 3 2)
    cap noi parquet use using test.parquet, clear rg(5)
    cap noi parquet use using test.parquet, clear
    cap noi parquet use using test.parquet, clear rg(1 3 2)
    l
    cap noi parquet use tmp using test.parquet, clear
    l
}

* Row groups
* ----------

if ( `:list posof `"python"' in 0' == 0 ) {
    cap noi parquet use using testrg.parquet, clear
    l
    cap noi parquet use using testrg.parquet, clear rg(1 3)
    l
    cap noi parquet use using testrg.parquet, clear rg(1 2) highlevel
    l

    cap noi parquet use using testrg.parquet, clear rg(10)
    cap noi parquet use using testrg.parquet, clear rg(10) highlevel
    cap noi parquet use using testrg.parquet, clear rg(1 2 3 1 3)
    l
    cap noi parquet use using testrg.parquet, clear rg(1 3 1 4 3) highlevel
    l

    cap noi parquet use using testrg.parquet, clear rg(1 3) in(2 / 3) highlevel
    l
    cap noi parquet use using testrg.parquet, clear rg(2 4) in(1 / 6) highlevel
    cap noi parquet use using testrg.parquet, clear rg(2 4) in(1 / 3) highlevel
    l

    cap noi parquet use using testrg.parquet, clear rg(1 3) in(2 / 3) lowlevel
    l
    cap noi parquet use using testrg.parquet, clear rg(2 4) in(1 / 6) lowlevel
    cap noi parquet use using testrg.parquet, clear rg(2 4) in(1 / 3) lowlevel
    l
}

* README
* ------

set linesize 128
sysuse auto, clear
parquet save auto.parquet if foreign, replace
parquet use auto.parquet, clear
assert foreign == 1

sysuse auto, clear
parquet save auto.parquet if log(price) < 9.5 & inlist(rep78, 1, 2, 3), replace
parquet use auto.parquet, clear
assert (log(price) < 9.5) & inlist(rep78, 1, 2, 3)

sysuse auto, clear
replace gear_ratio = .a in 2
parquet save auto.parquet, replace
parquet use auto.parquet, clear
desc

parquet use price make gear_ratio using auto.parquet, clear
desc
parquet save gear_ratio make using auto.parquet, replace

sysuse auto, clear
parquet save auto.parquet, replace
parquet use auto.parquet, clear highlevel
drop rep78
parquet save auto.parquet, replace lowlevel
desc

parquet use price make foreign using auto.parquet, clear highlevel
desc
parquet save foreign make using auto.parquet, replace lowlevel

* Rest in range
* -------------

sysuse auto, clear
gen ix = _n
parquet save auto.parquet, replace
parquet use auto.parquet, clear lowlevel in(4/65)
desc
l make price rep78 gear* ix
parquet use auto.parquet, clear lowlevel in(42/44)
l
parquet use auto.parquet, clear highlevel in(42/44)
l
parquet use auto.parquet, clear lowlevel in(42)
l
parquet use auto.parquet, clear highlevel in(42)
l

* Basic
* -----

clear
set obs 10
gen byte   byte1    = _n * 1
gen int    int1     = _n * 8
gen long   long1    = _n * 42
gen float  float1   = runiform()
gen double double1  = rnormal()
gen str32  string32 = "something here" + string(_n)
desc
l
parquet save test-stata.parquet, replace lowlevel fixedlen

parquet use test-stata.parquet, clear
l
parquet use test-stata.parquet, clear highlevel
desc
compress
replace byte1       = . in 9
replace int1        = .b in 6
replace double1     = .a in 3
parquet save using test-stata2.parquet, replace
l
parquet use test-stata2.parquet, clear
desc
l
parquet use test-stata2.parquet, clear highlevel
desc
l
replace string32 = "" in 1 / 9
parquet save using test-stata2.parquet, replace
parquet use test-stata2.parquet, clear
desc
l
replace string32 = ""
replace string32 = "something longer this way comes" in 1
parquet save using test-stata2.parquet, replace
parquet use test-stata2.parquet, clear
desc
l

* read/write
* read/write lowlevel
* read/write varlist using in range
* asserts galore!

* Benchmarks
* ----------

set rmsg on
clear
set obs 5000000
gen float  x1 = runiform()
gen double x2 = rnormal()
gen long   l1 = 100 * int(runiform())
gen str7   s7 = cond(mod(_n, 123), "hello", "goodbye")
gen long   ix = _n
parquet save x2 using tmp.parquet, replace
parquet save tmp.parquet, replace
save tmp, replace
* export delimited using "tmp.csv", replace

if ( `c(MP)' ) parquet use tmp.parquet, clear threads(4) highlevel
parquet use x2 using tmp.parquet, clear

parquet use tmp.parquet, clear in(500000) highlevel verbose
disp _N, ix
parquet use tmp.parquet, clear in(-10/) highlevel verbose
disp _N, ix[1], ix[_N]

parquet use tmp.parquet, clear in(500000) lowlevel verbose
disp _N, ix
parquet use tmp.parquet, clear in(-10/) lowlevel verbose
disp _N, ix[1], ix[_N]

parquet use tmp.parquet, clear
use tmp.dta, clear
* import delimited using "tmp.csv", clear varn(1)
set rmsg off

* clear
* set obs 5000000
* forvalues i = 1 / 20 {
*     if ( mod(`i', 2) ) {
*         local type = cond(mod(`i', 5), "float", "double")
*         gen `type' x`i' = cond(mod(`i', 3), rnormal(), runiform())
*     }
*     else {
*         gen y`i' = cond(mod(`i', 4), "strings!", "more strings, but longer!")
*     }
* }
* set rmsg on
* save test.dta, replace
* use test.dta, clear
* parquet save test.parquet, replace
* parquet use test.parquet, clear
*
* !printf 'import pandas as pd\ndf = pd.read_parquet("test.parquet")' | python
* local stataParquet com.kylebarron.stataParquet.ParquetStataReader
* local jarParquet stataParquet-0.1.0-jar-with-dependencies.jar
* javacall `stataParquet' read,  jar(`c(sysdir_personal)'`jarParquet`) args("test.parquet")
*
* !rm -f test.dta
* !rm -f test.parquet

* Describe
* --------

parquet desc test.parquet
assert r(num_row_groups) == 4
parquet desc test.parquet, rg(2 4)
assert r(num_row_groups) == 2

parquet desc testrg.parquet
assert r(num_row_groups) == 4
parquet desc testrg.parquet, rg(2 4)
assert r(num_row_groups) == 2

parquet desc using auto.parquet
assert r(num_row_groups) == 1
parquet desc auto.parquet, in(10/ 20)
parquet desc price gear_ratio make using auto.parquet, in(10/ 20)

parquet desc using test-stata.parquet
parquet desc using test-stata2.parquet
