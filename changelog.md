Change Log
==========

## parquet-0.5.6 (2019-02-12)

### Enhancements

- Much faster read times with a relatively large number of variables.
  Previous versions allocated the number of observations and then the
  columns in Stata. Doing the reverse is orders of magnitudes faster.

## parquet-0.5.5 (2019-02-12)

### Features

- `progress(x)` option displays progress every `x` seconds.

## parquet-0.5.4 (2019-02-09)

### Features

- `parquet desc` allows the user to glean the contents of a parquet file.

## parquet-0.5.3 (2019-02-08)

### Features

- Closes #17. `parquet use, rg()` allows the user to specify the row
  groups to read.  For parquet datasets, each dataset in the folder is a
  group.

### Enhancements

- If fewer observations are read than the number expected, make a note
  of it and only keep as many observations as were read.

## parquet-0.5.2 (2019-01-30)

### Features

- `parquet save` now accepts `if`

## parquet-0.5.1 (2018-11-08)

### Features

- `parquet use` can concatenate every parquet file in a directory
  if a directory is passed instead of a file.

## parquet-0.5.0 (2018-11-08)

### Features

- `highlevel` reader can use option `in()`, but it is not as efficient.

## Notes

- reader scans the entire string column by default, which might be
  slower but I prefer lossless reads in this case.

## parquet-0.4.2 (2018-11-02)

### Features

- `lowlevel` back in; can use option `in()` to read in range using
  `lowlevel`.

## Notes

- `lowlevel` writer does not support missing values; user receives
  warning with option `lowlevel` and error if missing values are
  encountered.

## parquet-0.4.1 (2018-11-02)

### Enhancements

- High-level reader now supports missing values.
- The user is warned that extended missing values are coerced to NaN.
- `lowlevel` deprecated; moved to `debug_lowlevel`.
- User is warned if the data is empty (no obs).

## parquet-0.4.0 (2018-10-31)

### Features

- Added column selector to file reader.
- High-level file reader. Faster but might suffer from a loss of generality.
- Scan string fields (ByteArray) to figure out the maximum length.
  Optionally specify `nostrscan` to fall back to `strbuffer`; optionally
  specify `strscan(.)` to scan the entire column.

## parquet-0.3.0 (2018-10-31)

### Features

- Some FixedLen variable support using the low-level reader and writer.

## parquet-0.2.0 (2018-10-30)

### Bug fixes

- Install from scratch is now working on NBER severs.

## parquet-0.1.0 (2018-10-30)

### Features

- Read and write parquet files via `parquet read` and `parquet write`
