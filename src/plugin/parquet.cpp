/*********************************************************************
 * Program: parquet.cpp
 * Author:  Mauricio Caceres Bravo <mauricio.caceres.bravo@gmail.com>
 * Created: Fri Mar  3 19:42:00 EDT 2017
 * Updated: Sat Feb  9 12:39:13 EST 2019
 * Purpose: Stata plugin to read and write to the parquet file format
 * Note:    See stata.com/plugins for more on Stata plugins
 * Version: 0.5.4
 *********************************************************************/

/**
 * @file parquet.cpp
 * @author Mauricio Caceres Bravo
 * @date 09 Feb 2019
 * @brief Stata plugin
 *
 * This file should only ever be called from parquet.ado
 *
 * @see help parquet
 * @see http://www.stata.com/plugins for more on Stata plugins
 */

#include "spi/stplugin.h"
#include <string>
#include <array>

#include <cassert>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <locale>

#define DEBUG     0
#define VERBOSE   1

#include "reader_writer.h"
#include "parquet.h"
#include "parquet-utils.cpp"
#include "parquet-utils-multi.cpp"
#include "parquet-reader-ll.cpp"
#include "parquet-reader-hl.cpp"
#include "parquet-writer-ll.cpp"
#include "parquet-writer-hl.cpp"
#include "parquet-reader-ll-multi.cpp"

// Syntax
//     plugin call parquet varlist [if] [in], todo file.parquet [file.colnames]
//
// Scalars
// 
//     __sparquet_lowlevel
//     __sparquet_fixedlen
//     __sparquet_nrow
//     __sparquet_ncol
//     __sparquet_nread
//     __sparquet_strbuffer
//     __sparquet_rg_size
//     __sparquet_multi
//     __sparquet_verbose
//     __sparquet_if
//
// Matrices
//
//     __sparquet_coltypes
//     __sparquet_rowgroups

STDLL stata_call(int argc, char *argv[])
{
    ST_retcode rc = 0;
    int64_t flength, strbuffer, lowlevel, multi, verbose, ifobs;

    SPARQUET_CHAR(todo, 16);
    strcpy (todo, argv[0]);

    flength = strlen(argv[1]) + 1;
    SPARQUET_CHAR (fname, flength);
    strcpy (fname, argv[1]);

    if ( (rc = sf_scalar_int("__sparquet_strbuffer", 20, &strbuffer)) ) goto exit;
    if ( (rc = sf_scalar_int("__sparquet_lowlevel",  20, &lowlevel))  ) goto exit;
    if ( (rc = sf_scalar_int("__sparquet_multi",     16, &multi))     ) goto exit;
    if ( (rc = sf_scalar_int("__sparquet_verbose",   18, &verbose))   ) goto exit;
    if ( (rc = sf_scalar_int("__sparquet_if",        13, &ifobs))     ) goto exit;

    /**************************************************************************
     * This is the main wrapper. We apply one of:                             *
     *                                                                        *
     *     - check:     Exit with 0 status. This just tests the plugin can be *
     *                  called from Stata without crashing.                   *
     *                                                                        *
     *     - shape:     (read) Number of rows and columns                     *
     *     - colnames:  (read) Put column names into file                     *
     *     - coltypes:  (read) Put column types into matrix                   *
     *     - read:      Read parquet file into Stata                          *
     *                                                                        *
     *     - write:     Write Stata varlist to parquet file                   *
     *                                                                        *
     **************************************************************************/

    // We write column names to a temporary file and read it back to Stata

    sf_printf_debug(DEBUG, "Stata Parquet Debug: '%s' '%s'\n", todo, fname);
    if ( strcmp(todo, "check") == 0 ) {
        goto exit;
    }
    else if ( strcmp(todo, "shape") == 0 ) {
        if ( multi ) {
            sf_printf_debug(DEBUG, "\t(debug shape multi)\n");
            if ( (rc = sf_ll_shape_multi(fname, DEBUG)) ) goto exit;
        }
        else {
            sf_printf_debug(DEBUG, "\t(debug shape)\n");
            if ( (rc = sf_ll_shape(fname, DEBUG)) ) goto exit;
        }
    }
    else if ( strcmp(todo, "colnames") == 0 ) {
        flength = strlen(argv[2]) + 1;
        SPARQUET_CHAR (fcols, flength);
        strcpy (fcols, argv[2]);
        if ( multi ) {
            if ( (rc = sf_ll_colnames_multi(fname, fcols, DEBUG)) ) goto exit;
        }
        else {
            if ( (rc = sf_ll_colnames(fname, fcols, DEBUG)) ) goto exit;
        }
    }
    else if ( strcmp(todo, "coltypes") == 0 ) {
        // TODO: How to discern string from binary in ByteArray?
        if ( multi ) {
            if ( (rc = sf_ll_coltypes_multi(fname, strbuffer, DEBUG)) ) goto exit;
        }
        else {
            if ( (rc = sf_ll_coltypes(fname, strbuffer, DEBUG)) ) goto exit;
        }
    }
    else if ( strcmp(todo, "read") == 0 ) {
        if ( lowlevel ) {
            if ( multi ) {
                if ( (rc = sf_ll_read_varlist_multi(fname, verbose, DEBUG, strbuffer)) ) goto exit;
            }
            else {
                if ( (rc = sf_ll_read_varlist(fname, verbose, DEBUG, strbuffer)) ) goto exit;
            }
        }
        else {
            if ( (rc = sf_hl_read_varlist(fname, verbose, DEBUG, strbuffer)) ) goto exit;
        }
    }
    else if ( strcmp(todo, "write") == 0 ) {
        // TODO: Support strL as ByteArray?
        flength = strlen(argv[2]) + 1;
        SPARQUET_CHAR (fcols, flength);
        strcpy (fcols, argv[2]);
        if ( ifobs ) {
            if ( lowlevel ) {
                if ( (rc = sf_ll_write_varlist_if(fname, fcols, verbose, DEBUG, strbuffer)) ) goto exit;
            }
            else {
                if ( (rc = sf_hl_write_varlist_if(fname, fcols, verbose, DEBUG, strbuffer)) ) goto exit;
            }
        }
        else {
            if ( lowlevel ) {
                if ( (rc = sf_ll_write_varlist(fname, fcols, verbose, DEBUG, strbuffer)) ) goto exit;
            }
            else {
                if ( (rc = sf_hl_write_varlist(fname, fcols, verbose, DEBUG, strbuffer)) ) goto exit;
            }
        }
    }
    else {
        sf_printf_debug(verbose, "Nothing to do\n");
        rc = 198;
        goto exit;
    }

exit:
    return (rc) ;
}
