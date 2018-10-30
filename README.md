stata-parquet
=============

Read and write parquet files from Stata (Linux/Unix only).

This package uses the Apache Arrow C++ library to read and write parquet
files from Stata using plugins. Currently this package is only available
in Stata for Unix (Linux).

`version 0.1.0 30Oct2018`

Installation
------------

You need to install the Apache Arrow C++ library. In particular you will
need.

- `libarrow.so`
- `libparquet.so`

Installed. Suppose they are in `/usr/local/lib64` (which is the
case for me). You must start stata with either
```bash
export LD_LIBRARY_PATH=/usr/local/lib64
stata
```

or 
```bash
LD_LIBRARY_PATH=/usr/local/lib64 stata
```

Then, from your stata session (13.1 and above only), run
```stata
local github "https://raw.githubusercontent.com"
net install parquet, from(`github'/mcaceresb/stata-parquet/master/build/)
```

Usage
-----

```stata
sysuse auto
parquet save auto.parquet, replace
parquet use auto.parquet,  clear
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

Compiling
---------

__*Pending*__

TODO
----

Adequately deal with (or warn the user about):

[ ] Variable formats
[ ] Variable labels
[ ] Value labels
[ ] `strL` variables
[ ] Regular missing values
[ ] Extended missing values
[ ] Dataset notes
[ ] Variable characteristics
[ ] Option `skip` for columns that are in other formats?
[ ] No variables (raise error)
[ ] No obs (raise error)
[ ] Boolean format from Stata?
[ ] Automagically detect when ByteArray data are string vs binary? `str#` vs `strL`.

Improve:

[ ] Best way to transpose from column order to row order
