/*
 * This file is in the public domain, so clarified as of
 * 2006-07-17 by Arthur David Olson.
 *
 * IDENTIFICATION
 *	  src/common/timezone/scheck.cpp
 */

#include "postgres_fe.h"

#include "private.h"

const char* scheck(const char* string, const char* format)
{
    char* fbuf = NULL;
    const char* fp = NULL;
    char* tp = NULL;
    int c;
    const char* result = NULL;
    char dummy;

    result = "";
    if (string == NULL || format == NULL) {
        return result;
    }
    fbuf = imalloc((int)(2 * strlen(format) + 4));
    if (fbuf == NULL) {
        return result;
    }
    fp = format;
    tp = fbuf;
    while ((*tp++ = c = *fp++) != '\0') {
        if (c != '%') {
            continue;
        }
        if (*fp == '%') {
            *tp++ = *fp++;
            continue;
        }
        *tp++ = '*';
        if (*fp == '*') {
            ++fp;
        }
        while (is_digit(*fp)) {
            *tp++ = *fp++;
        }
        if (*fp == 'l' || *fp == 'h') {
            *tp++ = *fp++;
        } else if (*fp == '[') {
            do {
                *tp++ = *fp++;
            } while (*fp != '\0' && *fp != ']');
        }
        if ((*tp++ = *fp++) == '\0') {
            break;
        }
    }
    *(tp - 1) = '%';
    *tp++ = 'c';
    *tp = '\0';
    if (sscanf_s(string, fbuf, &dummy, 1) != 1) {
        result = (char*)format;
    }
    ifree(fbuf);
    return result;
}
