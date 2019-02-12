*! version 0.5.5 12Feb2019 Mauricio Caceres Bravo, mauricio.caceres.bravo@gmail.com
*! Parquet file reader and writer

* Return codes
* -1:    Parquet library error (see log for details)
* 17042: Feature not implemented (generic)
* 17100: Unsupported type (generic)
* 17101: Unsupported type (int96)
* 17102: Unsupported type (FixedLenByteArray)
* 17103: Unsupported type (str#)
* 17104: Unsupported type (strL)
* 17201: Inconsistent column types
* 17301: Invalid row group (not in range)
* 17302: Invalid row group (not in range)
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
    else if ( inlist(`"`todo'"', "desc", "descr", "descri", "describ", "describe") ) {
        if ( strpos(`"`0'"', "using") ) {
            parquet_desc `0'
        }
        else {
            parquet_desc using `0'
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
    syntax [namelist]            /// varlist to read; must be 'namelist' because the
                                 /// variables do not exist in memory.  The names must be
                                 /// the Stata equivalent of the parquet name. E.g. 'foo
                                 /// bar' must be foo_bar
                                 ///
           using/,               /// parquet file to read
    [                            ///
           clear                 /// clear the data in memory
           verbose               /// verbose
           progress(real 30)     /// progress every x seconds
           _check(int 10000)     /// check timer every _check obs
           rg(numlist)           /// read row groups
           ROWGroups(numlist)    /// read row groups
           in(str)               /// read in range
           highlevel             /// use the high-level reader
           lowlevel              /// use the low-level reader
           threads(int 1)        /// try multi-threading; high-level only
           strbuffer(int 65)     /// fall back to string buffer if length not parsed
           nostrscan             /// do not scan string lengths (use strbuffer)
           STRSCANner(real -1)   /// scan string lengths (ever obs)
    ]

    if ( `progress' <= 0 | `progress' >= . ) {
        disp as err "invalid number of seconds in progress()"
        exit 198
    }

    qui desc, short
    if ( `r(changed)' & `"`clear'"' == "" ) {
        error 4
    }

    * Read row groups
    if ( (`"`rg'"' != "") & (`"`rowgroups'"' != "") ) {
        disp as err "rg() and rowgroups() are aliases; specify only one."
        exit 198
    }
    else if ( `"`rowgroups'"' != "" ) {
        local rg: copy local rowgroups
    }
    else if ( `"`rg'"' == "" ) {
        local rg none
    }

    * TODO: Allow repeat values for fast append?
    * NOTE: MUST be sorted
    local rg: list uniq rg
    local rg: list sort rg

    if ( `"`rg'"' == `"none"' ) {
        mata __sparquet_rowgix = 1
    }
    else {
        mata __sparquet_rowgix = strtoreal(tokens(st_local("rg")))

        cap mata assert(sum(round(__sparquet_rowgix) :!= __sparquet_rowgix) :== 0)
        if ( _rc ) {
            disp as err "Invalid row groups passed to rg()"
            clean_exit
            exit 198
        }

        cap mata assert(sum(__sparquet_rowgix :<= 0) == 0)
        if ( _rc ) {
            disp as err "Invalid row groups passed to rg()"
            clean_exit
            exit 198
        }

        cap mata assert(sum(__sparquet_rowgix :>= .) == 0)
        if ( _rc ) {
            disp as err "Invalid row groups passed to rg()"
            clean_exit
            exit 198
        }
    }
    mata st_matrix("__sparquet_rowgix", __sparquet_rowgix)

    * Try to read directory; otherwise it is a file
    local cwd `"`c(pwd)'"'
    cap cd `"`using'"'
    if ( _rc ) {
        confirm file `"`using'"'
        local multi
    }
    else {
        local multi multi
        qui cd `"`cwd'"'
        local files: dir `"`using'"' files "*.parquet"
        foreach file of local files {
            confirm file `"`using'/`file'"'
        }
        local filedir: copy local using
        tempfile using
        mata: __sparquet_filenames = sort(tokens(`"`files'"')', 1)
        if ( `"`rg'"' != `"none"' ) {
            cap mata: assert(max(__sparquet_rowgix) <= rows(__sparquet_filenames))
            if ( _rc ) {
                mata: errprintf("group %g not allowed; parquet dataset only has %g groups", /*
                             */ max(__sparquet_rowgix), rows(__sparquet_filenames))
                clean_exit
                exit 198
            }
            mata: __sparquet_filenames = __sparquet_filenames[__sparquet_rowgix]
            mata: st_local("files", invtokens(__sparquet_filenames'))
        }
        mata: __sparquet_putfilenames(`"`using'"', `"`filedir'"', __sparquet_filenames)
    }

    * Misc options
    if ( "`highlevel'" == "" ) local lowlevel lowlevel

    if ( `strscanner' == -1 ) local strscanner .

    if ( (`"`in'"' != "") & (`"`lowlevel'"' == "") ) {
        disp as err "Warning: Option in() not efficient with -highlevel-"
    }

    if ( (`"`multi'"' != "") & (`"`lowlevel'"' == "") ) {
        disp as err "Multi-file reading not available with -highlevel-"
        clean_exit
        exit 198
    }

    * TODO: this only seems to work in Stata/MP; is it also limited to
    * the # of processors in Stata? It seems to slow it down quite a
    * bit, so maybe take out?

    if ( (`threads' > 1) & ("`lowlevel'" != "") ) {
        disp as err "Option -threads()- not available with -lowlevel-"
        clean_exit
        exit 198
    }
    if ( (`threads' > 1) & !`c(MP)' ) {
        disp as err "Option -threads()- only available with Stata/MP"
        clean_exit
        exit 198
    }
    if ( `threads' < 1 ) {
        disp as err "Specify a valid number of threads"
        clean_exit
        exit 198
    }
    if ( `threads' > 1 ) {
        disp as err "{bf:Warning:} Option -threads()- is experimental and often slower."
    }

    * Initialize scalars
    * ------------------

    scalar __sparquet_if        = 0
    scalar __sparquet_verbose   = `"`verbose'"' != ""
    scalar __sparquet_multi     = `"`multi'"' != ""
    scalar __sparquet_lowlevel  = `"`lowlevel'"' != ""
    scalar __sparquet_fixedlen  = `"`fixedlen'"' != ""
    scalar __sparquet_strbuffer = `strbuffer'
    scalar __sparquet_threads   = `threads'
    scalar __sparquet_nbytes    = .
    scalar __sparquet_ngroup    = .
    scalar __sparquet_nrow      = .
    scalar __sparquet_ncol      = .
    scalar __sparquet_nread     = .
    scalar __sparquet_progress  = `progress'
    scalar __sparquet_check     = `_check'
    scalar __sparquet_readrg    = cond(`"`rg'"' == `"none"', 0, `:list sizeof rg')
    matrix __sparquet_coltypes  = .
    matrix __sparquet_rawtypes  = .

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
    check_matsize, nvars(`=scalar(__sparquet_ncol)')

    * Parse in range
    * --------------

    gettoken infrom into: in,   p(/)
    gettoken slash  into: into, p(/)

    if ( "`infrom'" == "f" ) local infrom 1
    if ( "`infrom'" !=  "" ) {
        confirm number `infrom'
        if ( `infrom' == 0 ) local infrom 1
        if ( `infrom' < 0 ) {
            _assert(inlist("`into'", "l", "")), msg("Cannot read from a negative number")
        }
    }

    if ( "`into'" == "l" ) local into
    if ( "`into'" != "" ) {
        confirm number `into'
        if ( `into' < 0 ) {
            disp as err "Cannot read from a negative number"
            clean_exit
            exit 198
        }
    }

    if ( `"`infrom'"' == "" ) local infrom 1
    if ( `"`into'"' == "" ) {
        local into = cond((`"`slash'"' == "") & (`"`in'"' != ""), `infrom', `=scalar(__sparquet_nrow)')
    }
    if ( `"`in'"' != "" ) {
        if ( `infrom' < 0 ) {
            if ( `into' < 0 ) {
                local infrom = `=scalar(__sparquet_nrow)' + `into' + 1
                local into   = `infrom'
            }
            else {
                local infrom = `into' + `infrom' + 1
            }
        }
    }
    if ( `into' > `=scalar(__sparquet_nrow)' ) {
        disp as err "Tried to read until row `into' but data had `=scalar(__sparquet_nrow)' rows."
        clean_exit
        exit 198
    }
    scalar __sparquet_infrom = `infrom'
    scalar __sparquet_into   = `into'

    * Column names
    * ------------

    tempfile colnames
    cap noi plugin call parquet_plugin, colnames `"`using'"' `"`colnames'"'
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
    mata: __sparquet_colnames = __sparquet_getcolnames(`"`colnames'"')
    mata: __sparquet_colix    = __sparquet_getcolix(__sparquet_colnames, tokens(`"`namelist'"'))
    mata: __sparquet_varnames = __sparquet_makenames(__sparquet_colnames[__sparquet_colix])
    mata: st_matrix("__sparquet_colix", rowshape(__sparquet_colix, 1))
    mata: st_numscalar("__sparquet_ncol", length(__sparquet_colix))

    * Column types
    * ------------

    * byte:   -1, Int32, Boolean
    * int:    -2, Int32
    * long:   -3, Int32
    * float:  -4, Float
    * double: -5, Double, Int64
    * str#:    #, ByteArray
    * strL:    #? .?, not yet implemented

    if ( "`strscan'" != "nostrscan" ) {
        if ( `strscanner' == . ) {
            local strscanner = `=scalar(__sparquet_nrow)'
        }
        else if ( `strscanner' > 0 ) {
            if ( `strscanner' < 1 ) {
                local strscanner = ceil(`strscanner' * `=scalar(__sparquet_nrow)')
            }
            else {
                local strscanner = ceil(`strscanner')
            }
        }
        else if ( `strscanner' == 0 ){
            local strscanner = 0
        }
        else {
            local strscanner = `=2^16'
        }
    }
    else {
        local strscanner = 0
    }
    scalar __sparquet_strscan = `strscanner'

    mata __sparquet_rawtypes = J(1, `=scalar(__sparquet_ncol)', .)
    mata __sparquet_coltypes = J(1, `=scalar(__sparquet_ncol)', .)
    mata st_matrix("__sparquet_coltypes", __sparquet_coltypes)
    mata st_matrix("__sparquet_rawtypes", __sparquet_rawtypes)
    cap noi plugin call parquet_plugin, coltypes `"`using'"'
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
    mata __sparquet_coltypes = st_matrix("__sparquet_coltypes")
    mata __sparquet_rawtypes = st_matrix("__sparquet_rawtypes")

    * Generate empty dataset
    * ----------------------

    * TODO: How to code strL? Even possible?
    * TODO: How to parse column selector? Closest match?
    local ctypes
    local cnames
    forvalues j = 1 / `=scalar(__sparquet_ncol)' {
        mata: st_local("cname", __sparquet_varnames[`j'])
        mata: st_local("ctype", strofreal(__sparquet_coltypes[`j']))

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

    * Read parquet file!
    * ------------------

    clear
    qui set obs `=`into'-`infrom'+1'
    mata: (void) st_addvar(tokens("`ctypes'"), tokens("`cnames'"))
    forvalues j = 1 / `=scalar(__sparquet_ncol)' {
        mata: st_local("vlabel", __sparquet_colnames[`j'])
        mata: st_local("vname", __sparquet_varnames[`j'])
        label var `vname' `"`vlabel'"'
    }

    if ( "`multi'" == "multi" ) {
        disp _char(9), "Dir:     `filedir'"
        disp _char(9), "Groups:  `:list sizeof files'"
        disp _char(9), "Columns: `:list sizeof cnames'"
        disp _char(9), "Rows:    `=_N'"
        if ( "`verbose'" != "" ) disp ""
    }

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
    if ( `=scalar(__sparquet_nread) < _N' ) {
        qui keep in 1 / `=scalar(__sparquet_nread)'
    }

    clean_exit
    exit 0
end

* ---------------------------------------------------------------------
* Parquet writer

capture program drop parquet_write
program parquet_write
    syntax [varlist]         /// varlist to export
           using/            /// target dataset file
           [if]              /// export if condition
           [in],             /// export in range
    [                        ///
           progress(real 30) /// progress every x seconds
           _check(int 10000) /// check timer every _check obs
           replace           /// replace target file, if it exists
           verbose           /// verbose
           rgsize(real 0)    /// row-group size (should be large; default is N by nvars)
           lowlevel          /// (debugging only) use low-level writer
           fixedlen          /// (debugging only) export strings as fixed length
    ]

    if ( "`lowlevel'" != "" ) {
        disp as err "{bf:Warning:} Low-level parser should only be used for debugging."
        disp as err "{bf:Warning:} Low-level parser does not adequately write missing values."
    }

    if ( `progress' <= 0 | `progress' >= . ) {
        disp as err "invalid number of seconds in progress()"
        exit 198
    }

    * Parse plugin options
    * --------------------

    if ( ("`fixedlen'" != "") & ("`lowlevel'" == "") ) {
        disp as err "Option -fixedlen- requires option -lowlevel-"
        exit 198
    }
    if ( ("`fixedlen'" != "") & ("`lowlevel'" != "") ) {
        disp as txt "Warning: -fixedlen- will pad strings of varying width."
    }

    scalar __sparquet_if        = `"`if'"' != ""
    scalar __sparquet_verbose   = `"`verbose'"' != ""
    scalar __sparquet_multi     = 0
    scalar __sparquet_lowlevel  = `"`lowlevel'"' != ""
    scalar __sparquet_fixedlen  = `"`fixedlen'"' != ""
    scalar __sparquet_rg_size   = cond(`rgsize', `rgsize', `=_N * `:list sizeof varlist'')
    scalar __sparquet_strbuffer = 1
    scalar __sparquet_ncol      = `:list sizeof varlist'
    scalar __sparquet_readrg    = 0
    scalar __sparquet_progress  = `progress'
    scalar __sparquet_check     = `_check'
    scalar __sparquet_nbytes    = .
    scalar __sparquet_ngroup    = .
    matrix __sparquet_rowgix    = .
    matrix __sparquet_rawtypes  = .

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

    * Parse variable types
    * --------------------

    check_matsize, nvars(`=scalar(__sparquet_ncol)')
    mata __sparquet_coltypes = J(1, `=scalar(__sparquet_ncol)', .)

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
            clean_exit
            exit 17104
        }
        mata __sparquet_coltypes[`j'] = `ctype'
    }
    mata st_matrix("__sparquet_coltypes", __sparquet_coltypes)

    cap confirm file `"`using'"'
    if ( (_rc == 0) & ("`replace'" == "") ) {
        disp as err `"file '`using'' already exists"'
        clean_exit
        exit 602
    }

    * Write data into file
    * --------------------

    tempfile colnames
    mata: __sparquet_putcolnames(`"`colnames'"', tokens("`varlist'"))

    cap noi plugin call parquet_plugin `varlist' `if' `in', write `"`using'"' `"`colnames'"'
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
* Parquet Describer

capture program drop parquet_desc
program parquet_desc, rclass
    syntax [namelist]           /// varlist to read; must be 'namelist' because the
                                /// variables do not exist in memory.  The names must be
                                /// the Stata equivalent of the parquet name. E.g. 'foo
                                /// bar' must be foo_bar
                                ///
           using/,              /// parquet file to read
    [                           ///
           rg(numlist)          /// read row groups
           ROWGroups(numlist)   /// read row groups
           in(str)              /// read in range
           STRSCANner(real -1)  /// scan string lengths (default is 2^8 rows)
    ]

    * ---------------
    * Read row groups
    * ---------------

    if ( (`"`rg'"' != "") & (`"`rowgroups'"' != "") ) {
        disp as err "rg() and rowgroups() are aliases; specify only one."
        exit 198
    }
    else if ( `"`rowgroups'"' != "" ) {
        local rg: copy local rowgroups
    }
    else if ( `"`rg'"' == "" ) {
        local rg none
    }

    * TODO: Allow repeat values for fast append?
    * NOTE: MUST be sorted
    local rg: list uniq rg
    local rg: list sort rg

    if ( `"`rg'"' == `"none"' ) {
        mata __sparquet_rowgix = 1
    }
    else {
        mata __sparquet_rowgix = strtoreal(tokens(st_local("rg")))

        cap mata assert(sum(round(__sparquet_rowgix) :!= __sparquet_rowgix) :== 0)
        if ( _rc ) {
            disp as err "Invalid row groups passed to rg()"
            clean_exit
            exit 198
        }

        cap mata assert(sum(__sparquet_rowgix :<= 0) == 0)
        if ( _rc ) {
            disp as err "Invalid row groups passed to rg()"
            clean_exit
            exit 198
        }

        cap mata assert(sum(__sparquet_rowgix :>= .) == 0)
        if ( _rc ) {
            disp as err "Invalid row groups passed to rg()"
            clean_exit
            exit 198
        }
    }
    mata st_matrix("__sparquet_rowgix", __sparquet_rowgix)

    * ---------------------------------------------
    * Try to read directory; otherwise it is a file
    * ---------------------------------------------

    local cwd `"`c(pwd)'"'
    cap cd `"`using'"'
    if ( _rc ) {
        confirm file `"`using'"'
        local multi
    }
    else {
        local multi multi
        qui cd `"`cwd'"'
        local files: dir `"`using'"' files "*.parquet"
        foreach file of local files {
            confirm file `"`using'/`file'"'
        }
        local filedir: copy local using
        tempfile using
        mata: __sparquet_filenames = sort(tokens(`"`files'"')', 1)
        if ( `"`rg'"' != `"none"' ) {
            cap mata: assert(max(__sparquet_rowgix) <= rows(__sparquet_filenames))
            if ( _rc ) {
                mata: errprintf("group %g not allowed; parquet dataset only has %g groups", /*
                             */ max(__sparquet_rowgix), rows(__sparquet_filenames))
                clean_exit
                exit 198
            }
            mata: __sparquet_filenames = __sparquet_filenames[__sparquet_rowgix]
            mata: st_local("files", invtokens(__sparquet_filenames'))
        }
        mata: __sparquet_putfilenames(`"`using'"', `"`filedir'"', __sparquet_filenames)
    }

    * ------------------
    * Initialize scalars
    * ------------------

    scalar __sparquet_if        = 0
    scalar __sparquet_verbose   = 0
    scalar __sparquet_multi     = `"`multi'"' != ""
    scalar __sparquet_lowlevel  = 1
    scalar __sparquet_fixedlen  = 0
    scalar __sparquet_strbuffer = 255
    scalar __sparquet_threads   = 1
    scalar __sparquet_nbytes    = .
    scalar __sparquet_ngroup    = .
    scalar __sparquet_nrow      = .
    scalar __sparquet_ncol      = .
    scalar __sparquet_nread     = .
    scalar __sparquet_readrg    = cond(`"`rg'"' == `"none"', 0, `:list sizeof rg')
    matrix __sparquet_coltypes  = .
    matrix __sparquet_rawtypes  = .

    * ----------------------
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

    * --------------------------
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
    check_matsize, nvars(`=scalar(__sparquet_ncol)')

    * --------------
    * Parse in range
    * --------------

    gettoken infrom into: in,   p(/)
    gettoken slash  into: into, p(/)

    if ( "`infrom'" == "f" ) local infrom 1
    if ( "`infrom'" !=  "" ) {
        confirm number `infrom'
        if ( `infrom' == 0 ) local infrom 1
        if ( `infrom' < 0 ) {
            _assert(inlist("`into'", "l", "")), msg("Cannot read from a negative number")
        }
    }

    if ( "`into'" == "l" ) local into
    if ( "`into'" != "" ) {
        confirm number `into'
        if ( `into' < 0 ) {
            disp as err "Cannot read from a negative number"
            clean_exit
            exit 198
        }
    }

    if ( `"`infrom'"' == "" ) local infrom 1
    if ( `"`into'"' == "" ) {
        local into = cond((`"`slash'"' == "") & (`"`in'"' != ""), `infrom', `=scalar(__sparquet_nrow)')
    }
    if ( `"`in'"' != "" ) {
        if ( `infrom' < 0 ) {
            if ( `into' < 0 ) {
                local infrom = `=scalar(__sparquet_nrow)' + `into' + 1
                local into   = `infrom'
            }
            else {
                local infrom = `into' + `infrom' + 1
            }
        }
    }
    if ( `into' > `=scalar(__sparquet_nrow)' ) {
        disp as err "Tried to read until row `into' but data had `=scalar(__sparquet_nrow)' rows."
        clean_exit
        exit 198
    }
    scalar __sparquet_infrom = `infrom'
    scalar __sparquet_into   = `into'

    * ------------
    * Column names
    * ------------

    tempfile colnames
    cap noi plugin call parquet_plugin, colnames `"`using'"' `"`colnames'"'
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
    mata: __sparquet_colnames = __sparquet_getcolnames(`"`colnames'"')
    mata: __sparquet_colix    = __sparquet_getcolix(__sparquet_colnames, tokens(`"`namelist'"'))
    mata: __sparquet_varnames = __sparquet_makenames(__sparquet_colnames[__sparquet_colix])
    mata: st_matrix("__sparquet_colix", rowshape(__sparquet_colix, 1))
    mata: st_numscalar("__sparquet_ncol", length(__sparquet_colix))

    * ------------
    * Column types
    * ------------

    * byte:   -1, Int32, Boolean
    * int:    -2, Int32
    * long:   -3, Int32
    * float:  -4, Float
    * double: -5, Double, Int64
    * str#:    #, ByteArray
    * strL:    #? .?, not yet implemented

    if ( `strscanner' == . ) {
        local strscanner = `=scalar(__sparquet_nrow)'
    }
    else if ( `strscanner' > 0 ) {
        if ( `strscanner' < 1 ) {
            local strscanner = ceil(`strscanner' * `=scalar(__sparquet_nrow)')
        }
        else {
            local strscanner = ceil(`strscanner')
        }
    }
    else if ( `strscanner' == 0 ){
        local strscanner = 0
    }
    else {
        local strscanner = `=2^8'
    }
    scalar __sparquet_strscan = `strscanner'

    mata __sparquet_rawtypes = J(1, `=scalar(__sparquet_ncol)', .)
    mata __sparquet_coltypes = J(1, `=scalar(__sparquet_ncol)', .)
    mata st_matrix("__sparquet_coltypes", __sparquet_coltypes)
    mata st_matrix("__sparquet_rawtypes", __sparquet_rawtypes)
    cap noi plugin call parquet_plugin, coltypes `"`using'"'
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
    mata __sparquet_coltypes = st_matrix("__sparquet_coltypes")
    mata __sparquet_rawtypes = st_matrix("__sparquet_rawtypes")

    * -------------------------
    * Print dataset description
    * -------------------------

    * Raw types
    * 1 BOOLEAN
    * 2 INT32
    * 3 INT64
    * 4 INT96
    * 5 FLOAT
    * 6 DOUBLE
    * 7 BYTE_ARRAY
    * 8 FIXED_LEN_BYTE_ARRAY

    mata __sparquet_describe(    /*
        */ __sparquet_colix,     /*
        */ __sparquet_colnames,  /*
        */ __sparquet_rawtypes', /*
        */ __sparquet_varnames', /*
        */ __sparquet_coltypes')

    mata st_local("varlist",         invtokens(__sparquet_varnames))
    mata st_local("varlist_parquet", invtokens(__sparquet_colnames'))

    return local varlist         = "`varlist'"
    return local varlist_parquet = "`varlist_parquet'"

    return scalar k              = scalar(__sparquet_nrow)
    return scalar N              = scalar(__sparquet_ncol)
    return scalar num_row_groups = scalar(__sparquet_ngroup)
    return scalar width          = `bytes'

    clean_exit
    exit 0
end

cap mata: mata drop __sparquet_describe()
mata:
void function __sparquet_describe(
        real colvector colix,
        string colvector colnames,
        real colvector rawtypes,
        string colvector varnames,
        real colvector coltypes)
{
    real scalar i, l, anystr, bytes
    string scalar strsep, strfmt, strfmt2
    string colvector dstrHeadN
    string colvector dstrHeadL
    string colvector dstrHeadC
    string colvector dstrBodyL
    string colvector str_rawtypes, str_coltypes, str_colix
    real   colvector strlens

    str_colix    = J(rows(colix),    1, "")
    str_rawtypes = J(rows(rawtypes), 1, "")
    str_coltypes = J(rows(coltypes), 1, "")

    strlens      = J(6, 1, 0)
    dstrBodyL    = J(rows(varnames), 1, "")
    dstrHeadC    = J(4, 1, "")
    dstrHeadN    = J(3, 1, "")
    dstrHeadL    = J(3, 1, "")

    dstrHeadN[1] = strtrim(sprintf("%31.0fc", st_numscalar("__sparquet_nrow")))
    dstrHeadN[2] = strtrim(sprintf("%31.0fc", st_numscalar("__sparquet_ncol")))
    dstrHeadN[3] = strtrim(sprintf("%31.0fc", st_numscalar("__sparquet_ngroup")))

    strfmt = sprintf("%%%gs", max((strlen(dstrHeadN) \ 3)))
    dstrHeadL[1] = "   obs: " + sprintf(strfmt, dstrHeadN[1])
    dstrHeadL[2] = "  vars: " + sprintf(strfmt, dstrHeadN[2])
    dstrHeadL[3] = "groups: " + sprintf(strfmt, dstrHeadN[3])

    for (i = 1; i <= rows(colix); i++)  {
        str_colix[i] = sprintf("%g", colix[i])
    }

    for (i = 1; i <= rows(rawtypes); i++)  {
        if ( rawtypes[i] == 1 ) str_rawtypes[i] = "bool"
        if ( rawtypes[i] == 2 ) str_rawtypes[i] = "int32"
        if ( rawtypes[i] == 3 ) str_rawtypes[i] = "int64"
        if ( rawtypes[i] == 4 ) str_rawtypes[i] = "int96"
        if ( rawtypes[i] == 5 ) str_rawtypes[i] = "float"
        if ( rawtypes[i] == 6 ) str_rawtypes[i] = "double"
        if ( rawtypes[i] == 7 ) str_rawtypes[i] = "barray"
        if ( rawtypes[i] == 8 ) str_rawtypes[i] = "fl_barray"
    }

    bytes = 0
    anystr = 0
    for (i = 1; i <= rows(coltypes); i++)  {
        if ( coltypes[i] == -1 ) {
            str_coltypes[i] = "byte"
            bytes = bytes + 1
        }
        if ( coltypes[i] == -2 ) {
            str_coltypes[i] = "int"
            bytes = bytes + 2
        }
        if ( coltypes[i] == -3 ) {
            str_coltypes[i] = "long"
            bytes = bytes + 3
        }
        if ( coltypes[i] == -4 ) {
            str_coltypes[i] = "float"
            bytes = bytes + 4
        }
        if ( coltypes[i] == -5 ) {
            str_coltypes[i] = "double"
            bytes = bytes + 8
        }
        if ( coltypes[i]  >  0 ) {
            str_coltypes[i] = sprintf("str%g", coltypes[i])
            bytes = bytes + coltypes[i] + 1
            anystr = 1
        }
    }

    strlens[1] = 1 + max(strlen(str_colix) \ 6)
    strlens[2] = 1 + max(strlen(colnames) \ 4)
    strlens[3] = 1 + max(strlen(str_rawtypes) \ 4)
    strlens[4] = 1 + 4
    strlens[5] = 1 + max(strlen(varnames) \ 4)
    strlens[6] = 1 + max(strlen(str_coltypes) \ 4)

    strfmt = sprintf(" %%-%gs ", strlens[1])
    for (i = 2; i <= 6; i++) {
        strfmt = strfmt + sprintf(" %%-%gs", strlens[i])
    }

    strfmt2 = sprintf(                               /*
        */ "%%%gs %%s %%-%gs ",                      /*
        */ strlens[1] + strlens[2] + strlens[3] + 3, /*
        */ strlens[5] + strlens[6])

    dstrHeadC[1] = sprintf(strfmt2, "parquet", (strlens[4] - 1) * " ", "  stata")
    dstrHeadC[2] = sprintf(strfmt2, " ", "  -> ", " ")
    dstrHeadC[3] = sprintf(strfmt, /*
        */ "column", "name", "type", strlens[4] * " ", "column", "name")

    dstrHeadC[4] = sprintf(strfmt2,                          /*
        */ "-" * (strlens[1] + strlens[2] + strlens[3] + 4), /*
        */ " " * (strlens[4] - 1),                           /*
        */ "-" * (strlens[5] + strlens[6] + 2))

    for (i = 1; i <= rows(varnames); i++)  {
        dstrBodyL[i] = sprintf(strfmt, /*
            */ str_colix[i],    /*
            */ colnames[i],     /*
            */ str_rawtypes[i], /*
            */ " ",             /*
            */ varnames[i],     /*
            */ str_coltypes[i])
    }

    strsep = "-" * strlen(dstrHeadC[4])
    printf("%s\n", strsep)
    for (i = 1; i <= 3; i++)  {
        printf("%s\n", dstrHeadL[i])
    }
    printf("%s\n", strsep)
    for (i = 1; i <= 4; i++)  {
        printf("%s\n", dstrHeadC[i])
    }
    for (i = 1; i <= rows(varnames); i++)  {
        printf("%s\n", dstrBodyL[i])
    }
    printf("%s\n", strsep)

    printf("Size on disk: %s\n", /*
        */ strtrim(sprintf("%31.0fc", st_numscalar("__sparquet_nbytes"))))

    if ( anystr ) {
        if ( st_numscalar("__sparquet_strscan") < st_numscalar("__sparquet_nrow") ) {
            printf("Size in memory (when read; approximate): %s\n", /*
                */ strtrim(sprintf("%31.0fc", /*
                */ st_numscalar("__sparquet_nrow") * bytes)))
            printf("(note: String sizes are approximate; run with strscan(.) for exact sizes)\n")
        }
        else {
            printf("Size in memory (when read): %s\n", /*
                */ strtrim(sprintf("%31.0fc", /*
                */ st_numscalar("__sparquet_nrow") * bytes)))
        }
    }

    st_local("bytes", strofreal(bytes))
}
end

* ---------------------------------------------------------------------
* Aux functions

* Delete all scalar, matrices, mata objects, which are persistent across
* programs
capture program drop clean_exit
program clean_exit
    cap scalar drop __sparquet_if
    cap scalar drop __sparquet_nread
    cap scalar drop __sparquet_multi
    cap scalar drop __sparquet_nbytes
    cap scalar drop __sparquet_ngroup
    cap scalar drop __sparquet_nrow
    cap scalar drop __sparquet_ncol
    cap scalar drop __sparquet_strscan
    cap scalar drop __sparquet_strbuffer
    cap scalar drop __sparquet_threaded
    cap scalar drop __sparquet_lowlevel
    cap scalar drop __sparquet_fixedlen
    cap scalar drop __sparquet_rg_size
    cap scalar drop __sparquet_threads
    cap scalar drop __sparquet_infrom
    cap scalar drop __sparquet_into
    cap scalar drop __sparquet_progress
    cap scalar drop __sparquet_check

    cap matrix drop __sparquet_coltypes
    cap matrix drop __sparquet_rawtypes
    cap matrix drop __sparquet_colix
    cap matrix drop __sparquet_rowgix

    cap mata: mata drop __sparquet_rawtypes
    cap mata: mata drop __sparquet_coltypes
    cap mata: mata drop __sparquet_colix
    cap mata: mata drop __sparquet_colnames
    cap mata: mata drop __sparquet_varnames
    cap mata: mata drop __sparquet_rowgix
    cap mata: mata drop __sparquet_filenames
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
                _n(2) "{bf:failed. Will use mata-based workaround. Performance might suffer sharply.}"
            // di as err                                                        ///
            //     _n(1) "{bf:# variables > matsize (`nvars' > `c(matsize)').}" ///
            //     _n(2) "    {stata set matsize `=`nvars''}"                   ///
            //     _n(2) "{bf:failed. Try setting matsize manually.}"
            // exit 908
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
cap mata: mata drop __sparquet_getcolix()
cap mata: mata drop __sparquet_putcolnames()
cap mata: mata drop __sparquet_putfilenames()
cap mata: mata drop __sparquet_makenames()

* TODO: Selector matches multiple columns? Repeated selector?
mata:
string vector function __sparquet_getcolnames(string scalar fcol)
{
    string vector colnames
    string vector line
    real scalar i, ix
    scalar fh

    colnames = J(0, 1, "")
    fh = fopen(fcol, "r")
    while ( (line = fget(fh)) != J(0, 0, "") ) {
        colnames = colnames \ line
    }
    fclose(fh)

    return (colnames)
}

real vector function __sparquet_getcolix(string vector colnames, string vector sel)
{
    real vector colix
    real scalar i, ix

    colix = J(0, 1, .)
    if ( length(sel) > 0 ) {
        for (i = 1; i <= length(sel); i++) {
            if ( any(sel[i] :== colnames) ) {
                ix = selectindex(sel[i] :== colnames)
                colix = colix \ ix
            }
            else {
                errprintf("'%s' did not match any columns\n", sel[i])
                assert(0)
            }
        }
    }
    else {
        colix = 1::length(colnames)
    }
    return (colix);
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

void function __sparquet_putfilenames(
    string scalar fnames,
    string scalar filedir,
    string vector filenames)
{
    real scalar i
    scalar fh

    fh = fopen(fnames, "w")
    for (i = 1; i <= length(filenames); i++) {
        fwrite(fh, sprintf("%s\n", pathjoin(filedir, filenames[i])));
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
