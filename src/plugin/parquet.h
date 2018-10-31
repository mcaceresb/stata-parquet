#define BUF_MAX 4096

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
    sf_printf ("; %.3f seconds.\n", diff);
    *timer = clock();
}

#define SPARQUET_CHAR(cvar, len)    \
    char *cvar = new char[len]; \
    memset (cvar, '\0', sizeof(char) * len)
