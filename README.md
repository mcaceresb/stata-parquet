stata-parquet
=============

Read and write parquet files from Stata (Linux/Unix only).

This package uses the [Apache Arrow](https://github.com/apache/arrow)
C++ library to read and write parquet files from Stata using plugins.
Currently this package is only available in Stata for Unix (Linux).

`version 0.5.2 30Jan2019` | [Installation](#installation) | [Usage](#usage) | [Examples](#examples)

Installation
------------

You need to first install:

- The Apache Arrow C++ library.
- The GNU Compiler Collection
- The Boost C++ libraries.

### Installation with Conda

The easiest way to do that is via `conda` (see [here](https://conda.io/docs/user-guide/install/index.html) for instructions on installing Anaconda):
```bash
git clone https://github.com/mcaceresb/stata-parquet
cd stata-parquet
conda env create -f environment.yml
source activate stata-parquet

make GCC=${CONDA_PREFIX}/bin/g++ UFLAGS=-std=c++11 INCLUDE=${CONDA_PREFIX}/include LIBS=${CONDA_PREFIX}/lib all
stata -b "net install parquet, from(${PWD}/build) replace"
```

### Building Arrow/Parquet Manually

If you don't want to use Conda, you can also build Apache Arrow and Apache Parquet manually.

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
    make all
    stata -b "net install parquet, from(${PWD}/build) replace"
    ```

Usage
-----

### Usage with Conda

Activate the Conda environment with

```
source activate stata-parquet
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

`parquet save` and `parquet use` will save and load datasets in Parquet format, respectively. For example:

```stata
sysuse auto, clear
parquet save auto.parquet, replace
parquet use auto.parquet, clear
desc

parquet use price make gear_ratio using auto.parquet, clear in(10/20)
parquet save gear_ratio make using auto.parquet in 5/6 if price > 5000, replace
```

Note that the `if` clause is not supported by `parquet use`.

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

License
-------

`stata-parquet` is [MIT-licensed](https://github.com/mcaceresb/stata-parquet/blob/master/LICENSE).
