#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os import makedirs, path, linesep, chdir, system, remove, rename
from shutil import copy2, rmtree
from sys import platform
from tempfile import gettempdir
from zipfile import ZipFile
from re import search
import argparse

# ---------------------------------------------------------------------
# Command line parsing

parser = argparse.ArgumentParser()
parser.add_argument('--stata',
                    nargs    = 1,
                    type     = str,
                    metavar  = 'STATA',
                    default  = None,
                    required = False,
                    help     = "Path to stata executable")
parser.add_argument('--stata-args',
                    nargs    = 1,
                    type     = str,
                    metavar  = 'STATA_ARGS',
                    default  = None,
                    required = False,
                    help     = "Arguments to pass to Stata executable")
parser.add_argument('--make-flags',
                    nargs    = 1,
                    type     = str,
                    metavar  = 'MAKE_FLAGS',
                    default  = None,
                    required = False,
                    help     = "Arguments to pass to make")
parser.add_argument('--clean',
                    dest     = 'clean',
                    action   = 'store_true',
                    help     = "Clean build",
                    required = False)
parser.add_argument('--replace',
                    dest     = 'replace',
                    action   = 'store_true',
                    help     = "Replace build",
                    required = False)
parser.add_argument('--test',
                    dest     = 'test',
                    action   = 'store_true',
                    help     = "Run tests",
                    required = False)
parser.add_argument('--windows',
                    dest     = 'windows',
                    action   = 'store_true',
                    help     = "Compile for Windows from Unix environment.",
                    required = False)
args = vars(parser.parse_args())


# ---------------------------------------------------------------------
# Main program

def main():

    # ---------------------------------------------------------------------
    # Relevant files

    _ssc = [
        "parquet.ado",
        "parquet.sthlp"
    ]

    _zip = [
        "changelog.md",
        "parquet.pkg",
        "stata.toc"
    ] + _ssc

    _build = _zip + [
        "parquet_tests.do"
    ]

    # ---------------------------------------------------------------------
    # Run the script

    # Remove buld
    # -----------

    if args['clean']:
        print("Removing build files")
        for bfile in _build:
            try:
                remove(path.join("build", bfile))
                print("\tdeleted " + bfile)
            except:
                try:
                    remove(path.join("build", "parquet", bfile))
                    print("\tdeleted " + bfile)
                except:
                    print("\t" + bfile + " not found")

        rc = system("make clean")
        exit(0)

    makedirs_safe(path.join("build", "parquet"))
    makedirs_safe("releases")

    # Stata executable
    # ----------------

    # I don't have stata on my global path, so to make the script portable
    # I make it look for my local executable when Stata is not found.
    if args['stata'] is not None:
        statadir = path.abspath(".")
        stataexe = args['stata'][0]
        statargs = "-b do" if args['stata_args'] is None else args['stata_args'][0]
        statado  = '"{0}" {1}'.format(stataexe, statargs)
    elif which("stata") is None:
        statadir = path.expanduser("~/.local/stata13")
        stataexe = path.join(statadir, "stata")
        statargs = "-b do" if args['stata_args'] is None else args['stata_args']
        statado  = '"{0}" {1}'.format(stataexe, statargs)
    else:
        statadir = path.abspath(".")
        stataexe = 'stata'
        statargs = "-b do" if args['stata_args'] is None else args['stata_args']
        statado  = '"{0}" {1}'.format(stataexe, statargs)

    statado = 'LD_LIBRARY_PATH=/usr/local/lib64 ' + statado

    # Temporary files
    # ---------------

    maindir   = path.dirname(path.realpath(__file__))
    tmpdir    = gettempdir()
    tmpupdate = path.join(tmpdir, ".update_parquet.do")

    # Compile plugin files
    # --------------------

    if platform in ["linux", "linux2", "win32", "cygwin", "darwin"]:
        print("Trying to compile plugins for -parquet-")
        print("(note: this assumes you have already compiled SpookyHash)")
        make_flags = args['make_flags'][0] if args['make_flags'] is not None else ""
        rc = system("make {0}".format(make_flags))
        print("Success!" if rc == 0 else "Failed.")
        if args['windows']:
            rc = system("make EXECUTION=windows clean")
            rc = system("make EXECUTION=windows spooky")
            rc = system("make EXECUTION=windows {0}".format(make_flags))
    else:
        print("Don't know platform '{0}'; compile manually.".format(platform))
        exit(198)

    print("")

    # Get unit test files
    # -------------------

    with open(path.join("build", "parquet_tests.do"), 'w') as outfile:
        outfile.writelines(
            open(path.join("src", "test", "parquet_tests.do")).readlines()
        )

    # Copy files to ./build
    # ---------------------

    _dir = path.join("build", "parquet")
    copy2("changelog.md", _dir)
    copy2(path.join("src", "parquet.pkg"), _dir)
    copy2(path.join("src", "stata.toc"), _dir)
    copy2(path.join("src", "ado", "parquet.ado"), _dir)
    copy2(path.join("docs", "parquet.sthlp"), _dir)

    # Copy files to .zip folder in ./releases
    # ---------------------------------------

    # Get stata version
    with open(path.join("src", "ado", "parquet.ado"), 'r') as f:
        line    = f.readline()
        version = search('(\d+\.?)+', line).group(0)

    plugins = ["parquet_unix.plugin",
               "parquet_windows.plugin",
               "parquet_macosx.plugin"]
    plugbak = plugins[:]
    for plug in plugbak:
        if not path.isfile(path.join("build", plug)):
            alt = path.join("lib", "plugin", plug)
            if path.isfile(alt):
                copy2(alt, "build")
            else:
                print("Could not find '{0}'".format(plug))

    chdir("build")
    print("Compressing build files for parquet-{0}".format(version))
    if rc == 0:
        _anyplug = False
        for plug in plugbak:
            if path.isfile(plug):
                _anyplug = True
                rename(path.join(plug), path.join("parquet", plug))
            else:
                plugins.remove(plug)
                print("\t'{0}' not found; skipping.".format(plug))

        if not _anyplug:
            print("WARNING: Could not find plugins despite build exit with 0 status.")
            exit(-1)

        _zip += plugins
    else:
        print("WARNING: Failed to build plugins. Will exit.")
        exit(-1)

    outzip = path.join(maindir, "releases", "parquet-latest.zip".format(version))
    with ZipFile(outzip, 'w') as zf:
        for zfile in _zip:
            zf.write(path.join("parquet", zfile))
            print("\t" + path.join("parquet", zfile))
            rename(path.join("parquet", zfile), zfile)

    chdir(maindir)
    rmtree(path.join("build", "parquet"))

    # Copy files to send to SSC
    # -------------------------

    print("")
    print("Compressing build files for parquet-ssc.zip")
    if rc == 0:
        _ssc += plugins
    else:
        print("WARNING: Failed to build plugins. Will exit.")
        exit(-1)

    chdir("build")
    outzip = path.join(maindir, "releases", "parquet-ssc.zip")
    with ZipFile(outzip, 'w') as zf:
        for zfile in _ssc:
            zf.write(zfile)
            print("\t" + zfile)

    # Replace package in ~/ado/plus
    # -----------------------------

    chdir(maindir)
    if args["replace"]:
        if which(stataexe):
            with open(tmpupdate, 'w') as f:
                f.write("global builddir {0}".format(path.join(maindir, "build")))
                f.write(linesep)
                f.write("cap net uninstall parquet")
                f.write(linesep)
                f.write("net install parquet, from($builddir)")
                f.write(linesep)

            chdir(statadir)
            system(statado + " " + tmpupdate)
            remove(tmpupdate)
            # print(linesep + "Replaced parquet in ~/ado/plus")
            chdir(maindir)
        else:
            print("Could not find Stata executable '{0}'.".format(stataexe))
            exit(-1)

    # Run tests
    # ---------

    if args['test']:
        print("Running tests (see build/parquet_tests.log for output)")
        chdir("build")
        system(statado + " parquet_tests.do")
        chdir(maindir)


# ---------------------------------------------------------------------
# Aux programs

try:
    from shutil import which
except:
    def which(program):
        import os

        def is_exe(fpath):
            return path.isfile(fpath) and os.access(fpath, os.X_OK)

        fpath, fname = path.split(program)
        if fpath:
            if is_exe(program):
                return program
        else:
            for epath in os.environ["PATH"].split(os.pathsep):
                epath = epath.strip('"')
                exe_file = path.join(epath, program)
                if is_exe(exe_file):
                    return exe_file

        return None


def makedirs_safe(directory):
    try:
        makedirs(directory)
        return directory
    except OSError:
        if not path.isdir(directory):
            raise


# ---------------------------------------------------------------------
# Run the things

if __name__ == "__main__":
    main()
