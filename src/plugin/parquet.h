#define BUF_MAX 4096
#define SPARQUET_VERSION "0.6.4"

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>

void sf_printf_debug (const int debug, const char *fmt, ...)
{
    va_list args;
    va_start (args, fmt);
    char buf[BUF_MAX];
    vsprintf (buf, fmt, args);
    if (debug > 0) SF_display (buf);
    if (debug > 1) printf (buf);
    va_end (args);
}

void sf_printf (const char *fmt, ...)
{
    va_list args;
    va_start (args, fmt);
    char buf[BUF_MAX];
    vsprintf (buf, fmt, args);
    SF_display (buf);
    // printf (buf);
    va_end (args);
}

void sf_errprintf (const char *fmt, ...)
{
    va_list args;
    va_start (args, fmt);
    char buf[BUF_MAX];
    vsprintf (buf, fmt, args);
    SF_error (buf);
    va_end (args);
}

void sf_running_timer (clock_t *timer, const char *msg)
{
    double diff  = (double) (clock() - *timer) / CLOCKS_PER_SEC;
    sf_printf (msg);
    sf_printf (" (%.2f sec).\n", diff);
    *timer = clock();
}

std::ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
}

void sf_running_progress_read (
    clock_t *timer,
    clock_t *stimer,
    ST_double progress,
    int64_t r,
    int64_t nrow_groups,
    int64_t j,
    int64_t ncol,
    int64_t i,
    int64_t nobs,
    ST_double pct)
{
    ST_double diff  = (ST_double) (clock() - *timer) / CLOCKS_PER_SEC;
    ST_double sdiff = (ST_double) (clock() - *stimer) / CLOCKS_PER_SEC;

    if ( sdiff < progress )
        return;

    *stimer = clock();
    sf_printf("\tReading: %.1f%%, %.1fs (rg %ld / %ld > col %ld / %ld > obs %ld / %ld)\n",
              pct, diff, r, nrow_groups, j, ncol, i, nobs);

}

void sf_running_progress_write (
    clock_t *timer,
    clock_t *stimer,
    ST_double progress,
    int64_t j,
    int64_t ncol,
    int64_t i,
    int64_t nobs,
    ST_double pct)
{
    ST_double diff  = (ST_double) (clock() - *timer) / CLOCKS_PER_SEC;
    ST_double sdiff = (ST_double) (clock() - *stimer) / CLOCKS_PER_SEC;

    if ( sdiff < progress )
        return;

    *stimer = clock();
    sf_printf("\tWriting: %.1f%%, %.1fs (col %ld / %ld > obs %ld / %ld)\n",
              pct, diff, j, ncol, i, nobs);

}

#define SPARQUET_CHAR(cvar, len)    \
    char *cvar = new char[len]; \
    memset (cvar, '\0', sizeof(char) * len)
