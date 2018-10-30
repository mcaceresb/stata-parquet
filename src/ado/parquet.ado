*! version 0.1.0 05Mar2018
*! Testing CPP plugins

capture program drop parquet
program parquet
    clear
    set obs 2
    gen n  = .
    gen l  = " "
    gen ix = .
    cap noi plugin call parquet_test n l ix
    if ( _rc ) {
        error _rc
    }
    l

    * clear
    * set obs 10
    * gen vn = _n
    * gen vx = 123456 * _n
    * gen vs = "some"
    * cap noi plugin call parquet_test vn vx vs
end

if ( inlist("`c(os)'", "MacOSX") | strpos("`c(machine_type)'", "Mac") ) local c_os_ macosx
else local c_os_: di lower("`c(os)'")

cap program drop parquet_test
program parquet_test, plugin using("parquet_`c_os_'.plugin")
