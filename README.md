stata-parquet
=============

Read and write parquet files from Stata (Linux/Unix only).

This package uses the [Apache Arrow](https://github.com/apache/arrow)
C++ library to read and write parquet files from Stata using plugins.
Currently this package is only available in Stata for Unix (Linux).

`version 0.2.0 30Oct2018`

Installation
------------


You need to first install:

- The Apache Arrow C++ library.
- The GNU Compiler Collection
- The boost C++ libraries.

The easiest way to do that is via `conda` (see [here](https://conda.io/docs/user-guide/install/index.html) for instructions on installing Anaconda):
```bash
git clone https://github.com/mcaceresb/stata-parquet
cd stata-parquet
conda env create -f environment.yml
source activate stata-parquet

make GCC=${CONDA_PREFIX}/bin/g++ UFLAGS=-std=c++11 INCLUDE=${CONDA_PREFIX}/include LIBS=${CONDA_PREFIX}/lib all
stata -b "net install parquet, from(${PWD}/build) replace"
```

Usage
-----

Activate the Conda environment with

```
source activate stata-parquet
```

Then be sure to start Stata via
```bash
LD_LIBRARY_PATH=${CONDA_PREFIX}/lib xstata
```

(You could also run `export LD_LIBRARY_PATH=${CONDA_PREFIX}/lib` before each
session or add `${CONDA_PREFIX}/lib` to `LD_LIBRARY_PATH` in your `~/.bashrc`.
In both of these examples, make sure to replace `${CONDA_PREFIX}` with the
absolute path it represents.) Then, from Stata

```stata
sysuse auto
parquet save auto.parquet, replace
parquet use auto.parquet, clear
compress
desc
```

Limitations
-----------

This is an alpha release and there are several important limitations:

- String widths are not read from `.parquet` files. The plugin reads all
  strings in uniform width. Control this via option `strbuffer()`
- Writing `strL` variables are not supported.
- Reading `FixedLenByteArray` and `Int96` variables are not supported, as
  they have no direct Stata counterpart, as best I know.

See the TODO section for more.

TODO
----

Adequately deal with (or warn the user about):

- [ ] Variable formats
- [ ] Variable labels
- [ ] Value labels
- [ ] `strL` variables
- [ ] Regular missing values
- [ ] Extended missing values
- [ ] Dataset notes
- [ ] Variable characteristics
- [ ] Option `skip` for columns that are in other formats?
- [ ] No variables (raise error)
- [ ] No obs (raise error)
- [ ] Boolean format from Stata?
- [ ] Automagically detect when ByteArray data are string vs binary? `str#` vs `strL`.

Improve:

- [ ] Best way to transpose from column order to row order

LICENSE
-------

`stata-parquet` is [MIT-licensed](https://github.com/mcaceresb/stata-parquet/blob/master/LICENSE).
