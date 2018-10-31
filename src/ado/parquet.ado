*! version 0.2.0 30Oct2018 Mauricio Caceres Bravo, mauricio.caceres.bravo@gmail.com
*! Parquet file reader and writer

* Return codes
* -1:    Parquet library error (see log for details)
* 17042: Feature not implemented (generic)
* 17100: Unsupported type (generic)
* 17101: Unsupported type (int96)
* 17102: Unsupported type (FixedLenByteArray)
* 17103: Unsupported type (str#)
* 17104: Unsupported type (strL)
* 198:   Generic error
* Other: Stata error

* ---------------------------------------------------------------------
* Main wrapper

capture program drop parquet
program parquet
    gettoken todo 0: 0
    if ( inlist(`"`todo'"', "read", "use") ) {
        if ( strpos(`"`0'"', "using") ) {
            parquet_read `0'
        }
        else {
            parquet_read using `0'
        }
    }
    else if ( inlist(`"`todo'"', "write", "save") ) {
        if ( strpos(`"`0'"', "using") ) {
            parquet_write `0'
        }
        else {
            parquet_write using `0'
        }
    }
    else {
        disp as err `"Unknown sub-comand `todo'"'
        exit 198
    }
end

* ---------------------------------------------------------------------
* Parquet reader

capture program drop parquet_read
program parquet_read
    syntax using/, [clear strbuffer(int 255)]

    qui desc, short
    if ( `r(changed)' & `"`clear'"' == "" ) {
        error 4
    }

    * Initialize scalars
    * ------------------

    * TODO: All strings are initialized with length strbuffer; fix

    scalar __sparquet_strbuffer = `strbuffer'
    scalar __sparquet_nrow      = .
    scalar __sparquet_ncol      = .
    matrix __sparquet_coltypes  = .

    * Check plugin loaded OK
    * ----------------------

    cap noi plugin call parquet_plugin, check `"`using'"'
    if ( _rc == -1 ) {
        disp as err "Parquet library error."
        clean_exit
        exit -1
    }
    else if ( _rc ) {
        local rc = _rc
        disp as err "Parquet plugin not loaded correctly."
        clean_exit
        exit `rc'
    }

    * Number of rows and columns
    * --------------------------

    cap noi plugin call parquet_plugin, shape `"`using'"'
    if ( _rc == -1 ) {
        disp as err "Parquet library error."
        clean_exit
        exit 198
    }
    else if ( _rc ) {
        local rc = _rc
        disp as err "Unable to read column and row information."
        clean_exit
        exit `rc'
    }
    * scalar list
    check_matsize, nvars(`=scalar(__sparquet_ncol)')

    * Column types
    * ------------

    * byte:   -1, Int32, Boolean
    * int:    -2, Int32
    * long:   -3, Int32
    * float:  -4, Float
    * double: -5, Double, Int64
    * str#:    #, ByteArray
    * strL:    #? .?, not yet implemented

    tempfile colnames
    matrix __sparquet_coltypes = J(1, `=scalar(__sparquet_ncol)', .)
    cap noi plugin call parquet_plugin, coltypes `"`using'"' `"`colnames'"'
    if ( _rc == -1 ) {
        disp as err "Parquet library error."
        clean_exit
        exit 198
    }
    else if ( _rc ) {
        local rc = _rc
        disp as err "Unable to parse column info."
        clean_exit
        exit `rc'
    }
    * matrix list __sparquet_coltypes
    mata: __sparquet_colnames = __sparquet_getcolnames(`"`colnames'"')
    mata: __sparquet_varnames = __sparquet_makenames(__sparquet_colnames)

    * Generate empty dataset
    * ----------------------

    * TODO: How to code strL? Even possible?
    * TODO: How to code str#? Get max length of ByteArray?
    local ctypes
    local cnames
    forvalues j = 1 / `=scalar(__sparquet_ncol)' {
        mata: st_local("cname", __sparquet_varnames[`j'])
        local ctype = __sparquet_coltypes[1, `j']

             if ( `ctype' == -1 ) local cstr byte
        else if ( `ctype' == -2 ) local cstr int
        else if ( `ctype' == -3 ) local cstr long
        else if ( `ctype' == -4 ) local cstr float
        else if ( `ctype' == -5 ) local cstr double
        else if ( (`ctype' > 0) & (`ctype' <= 2045) ) local cstr str`ctype'
        else {
            disp as err "Unable to parse column types: Unknown type code."
            exit 198
        }
        local ctypes `ctypes' `cstr'
        local cnames `cnames' `cname'
        * local cnames `cnames' v`j'
    }

    check_reserved `cnames'
    local cnames `r(varlist)'

    * TODO: Figure out how to map arbitrary strings into unique stata variable names
    clear
    set obs `=scalar(__sparquet_nrow)'
    mata: (void) st_addvar(tokens("`ctypes'"), tokens("`cnames'"))
    forvalues j = 1 / `=scalar(__sparquet_ncol)' {
        mata: st_local("vlabel", __sparquet_colnames[`j'])
        mata: st_local("vname", __sparquet_varnames[`j'])
        label var `vname' `"`vlabel'"'
    }

    * Read parquet file!
    * ------------------

    cap noi plugin call parquet_plugin `cnames', read `"`using'"'
    if ( _rc == -1 ) {
        disp as err "Parquet library error."
        clean_exit
        exit 198
    }
    else if ( _rc ) {
        local rc = _rc
        disp as err "Unable to read parquet file into memory."
        clean_exit
        exit `rc'
    }
    * desc
    * l

    clean_exit
    exit 0
end

* ---------------------------------------------------------------------
* Parquet writer

capture program drop parquet_write
program parquet_write
    syntax [varlist] using/ [in], [replace rgsize(real 0)]

    scalar __sparquet_rg_size   = cond(`rgsize', `rgsize', `=_N * `:list sizeof varlist'')
    scalar __sparquet_strbuffer = 1
    scalar __sparquet_ncol      = `:list sizeof varlist'

    check_matsize, nvars(`=scalar(__sparquet_ncol)')
    matrix __sparquet_coltypes  = J(1, `=scalar(__sparquet_ncol)', .)

    // TODO: Support strL as ByteArray?
    forvalues j = 1 / `=scalar(__sparquet_ncol)' {
        local cstr = "`:type `:word `j' of `varlist'''"

        * byte:   -2, Int32 (Boolean is only 0/1)
        * int:    -2, Int32
        * long:   -3, Int32
        * float:  -4, Float
        * double: -5, Double, Int64
        * str#:    #, ByteArray
        * strL:    #? .?, not yet implemented

             if ( `"`cstr'"' == "byte"   ) local ctype = -2
        else if ( `"`cstr'"' == "int"    ) local ctype = -2
        else if ( `"`cstr'"' == "long"   ) local ctype = -3
        else if ( `"`cstr'"' == "float"  ) local ctype = -4
        else if ( `"`cstr'"' == "double" ) local ctype = -5
        else if ( regexm("`cstr'", "str([0-9]+)") ) {
            local ctype = `=regexs(1)'
            scalar __sparquet_strbuffer = max(`=scalar(__sparquet_strbuffer)', `ctype')
        }
        else {
            disp as err "Column type not supported: `cstr'"
            exit 17104
        }
        matrix __sparquet_coltypes[1, `j'] = `ctype'
    }

    cap confirm file `"`using'"'
    if ( (_rc == 0) & ("`replace'" == "") ) {
        disp as err `"file '`using'' already exists"'
        exit 602
    }

    tempfile colnames
    mata: __sparquet_putcolnames(`"`colnames'"', tokens("`varlist'"))

    cap noi plugin call parquet_plugin `varlist' `in', write `"`using'"' `"`colnames'"'
    if ( _rc == -1 ) {
        disp as err "Parquet library error."
        clean_exit
        exit 198
    }
    else if ( _rc ) {
        local rc = _rc
        disp as err "Unable to write parquet file from memory."
        clean_exit
        exit `rc'
    }
end

* ---------------------------------------------------------------------
* Aux functions

* Delete all scalar, matrices, mata objects, which are persistent across
* programs
capture program drop clean_exit
program clean_exit
    cap scalar drop __sparquet_nrow
    cap scalar drop __sparquet_ncol
    cap scalar drop __sparquet_strbuffer
    cap scalar drop __sparquet_rg_size
    cap matrix drop __sparquet_coltypes
    cap mata: mata drop __sparquet_colnames
    cap mata: mata drop __sparquet_varnames
end

* Expand matsize if need be
capture program drop check_matsize
program check_matsize
    syntax [anything], [nvars(int 0)]
    if ( `nvars' == 0 ) local nvars `:list sizeof anything'
    if ( `nvars' > `c(matsize)' ) {
        cap set matsize `=`nvars''
        if ( _rc ) {
            di as err                                                        ///
                _n(1) "{bf:# variables > matsize (`nvars' > `c(matsize)').}" ///
                _n(2) "    {stata set matsize `=`nvars''}"                   ///
                _n(2) "{bf:failed. Try setting matsize manually.}"
            exit 908
        }
    }
end

* Pass a list of names obtained from __sparquet_makenames() so all names
* are possibly valid
capture program drop check_reserved
program check_reserved, rclass
    syntax anything
    local varlist
    foreach v of local anything {
        cap confirm name `v'
        if ( _rc ) {
            local j = 0
            while ( `:list v in anything' ) {
                local v `v'`++j'
            }
        }
        local varlist `varlist' `v'
    }
    return local varlist = "`varlist'"
end

cap mata: mata drop __sparquet_getcolnames()
cap mata: mata drop __sparquet_putcolnames()
cap mata: mata drop __sparquet_makenames()

mata:
string vector function __sparquet_getcolnames(string scalar fcol)
{
    string vector colnames
    string vector line
    scalar fh

    colnames = J(0, 1, "")
    fh = fopen(fcol, "r")
    while ( (line = fget(fh)) != J(0, 0, "") ) {
        colnames = colnames \ line
    }
    fclose(fh)
    return (colnames)
}

void function __sparquet_putcolnames(
    string scalar fcol,
    string vector colnames)
{
    real scalar i
    scalar fh

    fh = fopen(fcol, "w")
    for (i = 1; i <= length(colnames); i++) {
        fwrite(fh, sprintf("%s\n", colnames[i]));
    }
    fclose(fh)
}

string vector function __sparquet_makenames(string vector labels)
{
    string vector colnames
    string scalar v, jstr
    real scalar i, j, l

    colnames = J(1, length(labels), "")
    for (i = 1; i <= length(labels); i++) {
        v = strtoname(labels[i])
        j = 1
        l = strlen(v)
        while ( any(colnames :== v) ) {
            jstr = strofreal(j)
            if ( l == 32 ) {
                v = substr(v, 1, 32 - strlen(jstr))
            }
            else if ( j > 1 ) {
                v = substr(v, 1, strlen(v) - strlen(jstr))
            }
            v = v :+ jstr
            j = j + 1
        }
        colnames[i] = v
    }
    return (colnames)
}
end

* ---------------------------------------------------------------------
* Load the plugin

if ( inlist("`c(os)'", "MacOSX") | strpos("`c(machine_type)'", "Mac") ) local c_os_ macosx
else local c_os_: di lower("`c(os)'")

cap program drop parquet_plugin
program parquet_plugin, plugin using("parquet_`c_os_'.plugin")
