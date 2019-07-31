stata-parquet
=============

Read and write parquet files from Stata (Linux/Unix only).

This package uses the [Apache Arrow](https://github.com/apache/arrow)
C++ library to read and write parquet files from Stata using plugins.
Currently this package is only available in Stata for Unix (Linux).

`version 0.6.1 31Jul2019` | [Installation](#installation) | [Usage](#usage) | [Examples](#examples)

Installation
------------

You need to first install:

- The Apache Arrow C++ library.
- The GNU Compiler Collection
- The Boost C++ libraries.

### Installation with Conda

The only currently tested way to install this software is via `conda` (see [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) for installation instructions; most recent plugin installation and tests were conducted using `conda 4.7.10` from Miniconda):

```bash
git clone https://github.com/mcaceresb/stata-parquet
cd stata-parquet
conda env create -f environment.yml
conda activate stata-parquet

make GCC=${CONDA_PREFIX}/bin/x86_64-conda_cos6-linux-gnu-g++ UFLAGS=-std=c++11 INCLUDE=${CONDA_PREFIX}/include LIBS=${CONDA_PREFIX}/lib all
stata -b "net install parquet, from(${PWD}/build) replace"
```

### Building Arrow/Parquet Manually

If you don't want to use Conda, you can try build Apache Arrow and Apache Parquet manually (__*NOTE:*__ This is untested with the most recent version of the plugin).

1. [Install dependencies for Arrow and Parquet](https://github.com/apache/arrow/tree/master/cpp#system-setup). On Ubuntu/Debian, you can use:

    ```bash
    sudo apt-get install        \
        autoconf                \
        build-essential         \
        cmake                   \
        libboost-dev            \
        libboost-filesystem-dev \
        libboost-regex-dev      \
        libboost-system-dev     \
        python                  \
        bison                   \
        flex
    ```

2. Build Arrow and Parquet:

    ```bash
    git clone https://github.com/apache/arrow.git
    cd arrow/cpp
    # Not sure if this is generally necessary; may be due to my firewall
    # This changes the Apache mirror to the one at utah.edu
    sed -i 's$http://archive.apache.org/dist/thrift$http://apache.cs.utah.edu/thrift$g' cmake_modules/ThirdpartyToolchain.cmake
    mkdir release
    cd release
    cmake -DARROW_PARQUET=ON  ..
    make arrow
    sudo make install
    make parquet
    sudo make install
    ```

3. Build/Install `stata-parquet`:

    ```bash
    git clone https://github.com/mcaceresb/stata-parquet
    cd stata-parquet
    make all UFLAGS=-std=c++11
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


### Usage with Manual Installation

Prepend the Arrow/Parquet installation directory to `LD_LIBRARY_PATH`. This
needs to be set when Stata starts so that Stata can find the Arrow/Parquet
dependencies. For me, this directory was `/usr/local/lib`.

```bash
LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH xstata
```

Alternatively, you could add the following line to your `~/.bashrc` to not have
to enter the `LD_LIBRARY_PATH` every time:

```bash
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
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
