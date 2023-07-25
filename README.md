stata-parquet
=============

Read and write parquet files from Stata (Linux/Unix only).

This package uses the [Apache Arrow](https://github.com/apache/arrow)
C++ library to read and write parquet files from Stata using plugins.
Currently this package is only available in Stata for Unix (Linux).

`version 0.6.4 12Aug2019` | [Installation](#installation) | [Usage](#usage) | [Examples](#examples)

Installation
------------

You need to first install:

- The Apache Arrow C++ library.
- The GNU Compiler Collection
- The Boost C++ libraries.
- Google's logging library (google-glog)

### Installation with Conda

First, intall Google's logging library (`libgoogle-glog-dev` in Ubuntu). Then the only tested way to install this software is via `conda` (see [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) for installation instructions; most recent plugin installation and tests were conducted using Miniconda3 for Python 3.8, version `23.3.1`):

```bash
git clone https://github.com/mcaceresb/stata-parquet
cd stata-parquet
git checkout dev-versioned
conda env create -f environment.yml
conda activate stata-parquet

make SPI=3.0 GCC=${CONDA_PREFIX}/bin/x86_64-conda_cos6-linux-gnu-g++ UFLAGS=-std=c++11 INCLUDE=${CONDA_PREFIX}/include LIBS=${CONDA_PREFIX}/lib all
stata -b "net install parquet, from(${PWD}/build) replace"
```

Usage
-----

### Usage with Conda

Activate the Conda environment with

```
conda activate stata-parquet
```

Then be sure to start Stata via
```bash
LD_LIBRARY_PATH=${CONDA_PREFIX}/lib:$LD_LIBRARY_PATH xstata
```

Alternatively, you could add the following line to your `~/.bashrc` to not have
to enter the `LD_LIBRARY_PATH` every time (make sure to replace
`${CONDA_PREFIX}` with the absolute path it represents):

```bash
export LD_LIBRARY_PATH=${CONDA_PREFIX}/lib:$LD_LIBRARY_PATH
```

Then just start Stata with

```
xstata
```

### Examples

`parquet save` and `parquet use` will save and load datasets in Parquet
format, respectively. `parquet desc` will describe the contents of a
parquet dataset. For example:

```stata
sysuse auto, clear
parquet save auto.parquet, replace
parquet desc auto.parquet
parquet use auto.parquet, clear
desc

parquet use price make gear_ratio using auto.parquet, clear in(10/20)
parquet save gear_ratio make using auto.parquet in 5/6 if price > 5000, replace
```

Note that the `if` clause is not supported by `parquet use`. To test the
plugin works as expected, run `do build/parquet_tests.do` from Stata. To
also test the plugin correctly reads `hive` format datasets, run

```
conda install -n stata-parquet pandas numpy fastparquet
conda activate stata-parquet
```

Then, from Stata, `do build/parquet_tests.do python`

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

- [ ] Option `skip` for columns that are in non-readable formats?
- [ ] Option to specify compression type
- [X] Write regular missing values (high-level only).

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

License
-------

`stata-parquet` is [MIT-licensed](https://github.com/mcaceresb/stata-parquet/blob/master/LICENSE).
