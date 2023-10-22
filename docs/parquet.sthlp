{smcl}
{* *! version 0.6.5 22Oct2023}{...}
{viewerdialog parquet "dialog parquet"}{...}
{vieweralsosee "[R] parquet" "mansection R parquet"}{...}
{viewerjumpto "Syntax" "parquet##syntax"}{...}
{viewerjumpto "Description" "parquet##description"}{...}
{viewerjumpto "Options" "parquet##options"}{...}
{viewerjumpto "Examples" "parquet##examples"}{...}
{title:Title}

{p2colset 5 17 17 2}{...}
{p2col :{cmd:parquet} {hline 2}}Read and write parquet files from Stata (Linux only){p_end}
{p2colreset}{...}

{marker syntax}{...}
{title:Syntax}

{pstd}
Read a parquet file into memory:

{p 8 15 2}
{cmd:parquet} use
{it:{help filename}}
{cmd:,} [{opt clear} {it:{help parquet##parquet_options:options}}]

{p 8 15 2}
{cmd:parquet} use
[{varlist}]
{cmd:using}
{it:{help filename}}
{cmd:,} [{opt clear} {it:{help parquet##parquet_options:options}}]

{pstd}
Save data in memory to a parquet file in disk:

{p 8 15 2}
{cmd:parquet} save
{it:{help filename}}
{cmd:,} [{opt replace} {it:{help parquet##parquet_options:options}}]

{p 8 15 2}
{cmd:parquet} save
[{varlist}]
{ifin}
{cmd:using}
{cmd:,} [{opt replace} {it:{help parquet##parquet_options:options}}]

{pstd}
Quick glean the contents of a parquet file or dataset:

{p 8 15 2}
{cmd:parquet} desc
{it:{help filename}}
{cmd:,} [{it:{help parquet##parquet_options:options}}]

{p 8 15 2}
{cmd:parquet} desc
[{varlist}]
{cmd:using}
{it:{help filename}}
{cmd:,} [{it:{help parquet##parquet_options:options}}]

{synoptset 18 tabbed}{...}
{marker parquet_options}{...}
{synopthdr}
{synoptline}
{syntab :Read}
{synopt :{opt clear}} Clear the data in memory.
{p_end}
{synopt :{opt in(from/to)}} Import observations in range.
{p_end}
{synopt :{opth rg(numlist)}} Row groups to read. {opt in()} is defined relative to the subset of row groups to be read.
{p_end}
{synopt :{opth progress(real)}} Display progress every x seconds.
{p_end}
{synopt :{opt nostrscan}} Do not pre-scan data for string width; falls back to {opt strbuffer}.
{p_end}
{synopt :{bf:strscan[(#)]}} Scan strings for max width. Default behavior scans every row.
{p_end}
{synopt :{opt strbuffer(#)}} Allocate string buffer of size {opt strbuffer} (strscan fallback).
{p_end}
{synopt :{opt highlevel}} Use the high-level reader instead of the low-level reader.
{p_end}

{syntab :Write}
{synopt :{opt replace}} Replace the target file.
{p_end}
{synopt :{opth rgsize(real)}} Use a row group size of {opt rgsize}; ignored with {opt lowlevel}.
{p_end}
{synopt :{opth chunkbytes(real)}} Chunk variable column if size exceeds {opt chunkbytes}.
{p_end}
{synopt :{opt fixedlen}} Export strings as fixed length; requires option {opt lowlevel}.
{p_end}
{synopt :{opt lowlevel}} Use the low-level writer instead of the high-level writer.
{p_end}
{synopt :{opt compression(str)}} Compression: SNAPPY (default), GZIP, LZO, BROTLI, LZ4, ZSTD, UNCOMPRESSED. Only with {opt lowlevel}.
{p_end}

{syntab :Describe}
{synopt :{opt in(from/to)}} Scan observations in range.
{p_end}
{synopt :{opth rg(numlist)}} Row groups to scan. {opt in()} is defined relative to the subset of row groups to be read.
{p_end}
{synopt :{bf:strscan[(#)]}} Scan strings for max width. Default behavior scans every row.
{p_end}

{p2colreset}{...}
{p 4 6 2}

{marker description}{...}
{title:Description}

{pstd}
This package uses the Apache Arrow C++ library to read and write parquet
files from Stata using plugins. Currently this package is only available
in Stata for Unix (Linux).

{marker example}{...}
{title:Examples}

{phang2}{cmd:. sysuse auto}{p_end}
{phang2}{cmd:. parquet save auto.parquet, replace}{p_end}
{phang2}{cmd:. parquet desc auto.parquet}{p_end}
{phang2}{cmd:. parquet use auto.parquet,  clear}{p_end}
{phang2}{cmd:. compress}{p_end}
{phang2}{cmd:. desc}{p_end}

{phang2}{cmd:. parquet use price make gear_ratio using auto.parquet, clear in(10/20)}{p_end}
{phang2}{cmd:. parquet save gear_ratio make using auto.parquet in 5/6, replace      }{p_end}

{marker author}{...}
{title:Author}

{pstd}Mauricio Caceres{p_end}
{pstd}{browse "mailto:mauricio.caceres.bravo@gmail.com":mauricio.caceres.bravo@gmail.com }{p_end}
{pstd}{browse "https://mcaceresb.github.io":mcaceresb.github.io}{p_end}

{pstd}
I am also the author of the {manhelp gtools R:gtools} project,
a collection of utilities for working with big data. See
{browse "https://github.com/mcaceresb/stata-gtools":github.com/mcaceresb/stata-gtools}{p_end}

{title:Also see}

{p 4 13 2}
{help gtools} (if installed)
