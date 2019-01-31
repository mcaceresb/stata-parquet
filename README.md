stata-parquet
=============

Read and write parquet files from Stata (Linux/Unix only).

This package uses the [Apache Arrow](https://github.com/apache/arrow)
C++ library to read and write parquet files from Stata using plugins.
Currently this package is only available in Stata for Unix (Linux).

`version 0.5.1 08Nov2018`

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

parquet use price make gear_ratio using auto.parquet, clear in(10/20)
parquet save gear_ratio make using auto.parquet in 5/6, replace
```

Limitations
-----------

- Writing `strL` variables is not yet supported.
- Reading binary ByteArray data is not supported, only strings.
- `Int96` variables is not supported, as is has no direct Stata counterpart.
- Maximum string widths are not generally stored in `.parquet` files (as
  far as I can tell). The default behavior is to scan string columns
  to get the largest string, but it can be time-intensive. Adjust this
  behavior via `strscan()` and `strbuffer()`.

TODO
----

Some features that ought to be implemented:

- [ ] `parquet desc`
- [ ] Option `skip` for columns that are in non-readable formats?
- [X] `parquet use` in range for low-level reader.
- [X] `parquet use` in range for high-level reader (though not as efficient).
- [X] Read regular missing values.
- [X] Write regular missing values (high-level only).
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
