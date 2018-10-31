/*********************************************************************
 * Program: parquet.cpp
 * Author:  Mauricio Caceres Bravo <mauricio.caceres.bravo@gmail.com>
 * Created: Fri Mar  3 19:42:00 EDT 2017
 * Updated: Tue Oct 30 17:46:27 EDT 2018
 * Purpose: Stata plugin to read and write to the parquet file format
 * Note:    See stata.com/plugins for more on Stata plugins
 * Version: 0.2.0
 *********************************************************************/

/**
 * @file parquet.cpp
 * @author Mauricio Caceres Bravo
 * @date 30 Oct 2018
 * @brief Stata plugin
 *
 * This file should only ever be called from parquet.ado
 *
 * @see help parquet
 * @see http://www.stata.com/plugins for more on Stata plugins
 */

#include "spi/stplugin.h"
#include <string.h>
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

const char SPARQUET_READER_VERSION[] = "v0.2.0";
const char SPARQUET_WRITER_VERSION[] = "v0.2.0";

#include "reader_writer.h"
#include "parquet.h"
#include "parquet-utils.cpp"
#include "parquet-reader-ll.cpp"
#include "parquet-writer-hl.cpp"

// Syntax
//     plugin call parquet varlist [in], todo file.parquet [file.colnames]
//
// Scalars
// 
//     __sparquet_nrow
//     __sparquet_ncol
//     __sparquet_strbuffer
//     __sparquet_rg_size
//
// Matrices
//
//     __sparquet_coltypes

STDLL stata_call(int argc, char *argv[])
{
    ST_retcode rc = 0;
    ST_double z;
    size_t flength, strbuffer;

    SPARQUET_CHAR(todo, 16);
    strcpy (todo, argv[0]);

    flength = strlen(argv[1]) + 1;
    SPARQUET_CHAR (fname, flength);
    strcpy (fname, argv[1]);

    /**************************************************************************
     * This is the main wrapper. We apply one of:                             *
     *                                                                        *
     *     - check:     Exit with 0 status. This just tests the plugin can be *
     *                  called from Stata without crashing.                   *
     *     - shape:     (read) Number of rows and columns                     *
     *     - coltypes:  (read) Put column types into matrix                   *
     *     - read:      Read parquet file into Stata                          *
     *     - write:     Write Stata varlist to parquet file                   *
     *                                                                        *
     **************************************************************************/

    // We write column names to a temporary file and read it back to Stata

    sf_printf_debug(DEBUG, "Stata Parquet Debug: '%s' '%s'\n", todo, fname);
    if ( strcmp(todo, "check") == 0 ) {
        goto exit;
    }
    else if ( strcmp(todo, "shape") == 0 ) {
        if ( (rc = sf_ll_shape(fname, DEBUG)) ) goto exit;
    }
    else if ( strcmp(todo, "coltypes") == 0 ) {
        // TODO: How to parse string type lengths?
        // TODO: How to discern string from binary in ByteArray?
        flength = strlen(argv[2]) + 1;
        SPARQUET_CHAR (fcols, flength);
        strcpy (fcols, argv[2]);

        SPARQUET_CHAR(vscalar, 32);
        memcpy(vscalar, "__sparquet_strbuffer", 20);
        if ( (rc = SF_scal_use(vscalar, &z)) ) goto exit;
        strbuffer = (size_t) z;

        if ( (rc = sf_ll_coltypes(fname, fcols, DEBUG, strbuffer)) ) goto exit;
    }
    else if ( strcmp(todo, "read") == 0 ) {
        SPARQUET_CHAR(vscalar, 32);
        memcpy(vscalar, "__sparquet_strbuffer", 20);
        if ( (rc = SF_scal_use(vscalar, &z)) ) goto exit;
        strbuffer = (size_t) z;

        sf_printf_debug(VERBOSE, "Stata Parquet Reader %s\n", SPARQUET_READER_VERSION);
        if ( (rc = sf_ll_read_varlist(fname, VERBOSE, DEBUG, strbuffer)) ) goto exit;
    }
    else if ( strcmp(todo, "write") == 0 ) {
        // TODO: Support strL as ByteArray?
        flength = strlen(argv[2]) + 1;
        SPARQUET_CHAR (fcols, flength);
        strcpy (fcols, argv[2]);

        SPARQUET_CHAR(vscalar, 32);
        memcpy(vscalar, "__sparquet_strbuffer", 20);
        if ( (rc = SF_scal_use(vscalar, &z)) ) goto exit;
        strbuffer = (size_t) z;

        sf_printf_debug(VERBOSE, "Stata Parquet Writer %s\n", SPARQUET_READER_VERSION);
        if ( (rc = sf_hl_write_varlist(fname, fcols, VERBOSE, DEBUG, strbuffer)) ) goto exit;
    }
    else {
        sf_printf_debug(VERBOSE, "Nothing to do\n");
        rc = 198;
        goto exit;
    }

exit:
    return (rc) ;
}
