Change Log
==========

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
