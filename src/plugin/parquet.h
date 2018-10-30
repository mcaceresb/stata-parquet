#define BUF_MAX 4096

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>

void sf_printf_debug (const char *fmt, ...)
{
    va_list args;
    va_start (args, fmt);
    char buf[BUF_MAX];
    vsprintf (buf, fmt, args);
    SF_display (buf);
    printf (buf);
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
