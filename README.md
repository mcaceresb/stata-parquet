stata-parquet
=============

Read and write parquet files from Stata (Linux/Unix only).

This package uses the [Apache Arrow](https://github.com/apache/arrow)
C++ library to read and write parquet files from Stata using plugins.
Currently this package is only available in Stata for Unix (Linux).

`version 0.4.1 02Nov2018`

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
sysuse auto, clear
parquet save auto.parquet, replace
parquet use auto.parquet, clear
desc

parquet use price make gear_ratio using auto.parquet, clear
parquet save gear_ratio make using auto.parquet, replace
```

Limitations
-----------

This is an alpha release and there are several important limitations:

- Maximum string widths are not generally stored in `.parquet` files
  (as far as I can tell). The default behavior is to try and guess the
  string length by scanning the first 2^16 rows of the file; control
  this via option `strscan()`.
- Writing `strL` variables are not supported.
- Reading binary ByteArray data is not supported, only strings.
- `Int96` variables is not supported, as is has no direct Stata counterpart.

See the TODO section for more.

TODO
----

Some features that ought to be implemented:

- [ ] `parquet desc`
- [X] Regular missing value support (high-level read/write only).
- [ ] Option `skip` for columns that are in non-readable formats?
- [X] No variables (raise error).
- [X] No obs (raise error).

Some features that might not be implementable, but the user should be
warned about them:

- [X] Extended missing values (user gets a warning).
- [ ] `strL` variables
- [ ] Variable formats
- [ ] Variable labels
- [ ] Value labels
- [ ] Dataset notes
- [ ] Variable characteristics
- [ ] ByteArray or FixedLenByteArray with binary data.

Improve:

- [ ] Boolean format to/from Stata.
- [ ] Best way to transpose from column order to row order.

LICENSE
-------

`stata-parquet` is [MIT-licensed](https://github.com/mcaceresb/stata-parquet/blob/master/LICENSE).
