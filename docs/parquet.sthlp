{smcl}
{* *! version 0.2.0 30Oct2018}{...}
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

{p 8 15 2}
{cmd:parquet} read {cmd:,} [{opt clear} {opth strbuffer(int)}]

{p 8 15 2}
{cmd:parquet} write {cmd:,} [{opt replace} {opth rgsize(int)}]

{synoptset 18 tabbed}{...}
{marker parquet_options}{...}
{synopthdr}
{synoptline}
{synopt :{opt clear}} When reading, clear the data in memory.
{p_end}
{synopt :{opt strbuffer(#)}} When reading, allocate string buffer of size {opt strbuffer}.
{p_end}
{synopt :{opt replace}} When writing, replace the target file.
{p_end}
{synopt :{opt rgsize}} When writing, use a row group size of {opt rgsize}.
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
{phang2}{cmd:. parquet use auto.parquet,  clear}{p_end}
{phang2}{cmd:. compress}{p_end}
{phang2}{cmd:. desc}{p_end}

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
