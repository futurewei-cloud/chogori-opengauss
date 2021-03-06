/* -------------------------------------------------------------------------
 *
 * tsvector.c
 *	  I/O functions for tsvector
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/tsvector.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "libpq/pqformat.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_utils.h"
#include "utils/memutils.h"

typedef struct {
    WordEntry entry; /* must be first! */
    WordEntryPos* pos;
    int poslen; /* number of elements in pos */
} WordEntryIN;

/* Compare two WordEntryPos values for qsort */
static int comparePos(const void* a, const void* b)
{
    int apos = WEP_GETPOS(*(const WordEntryPos*)a);
    int bpos = WEP_GETPOS(*(const WordEntryPos*)b);

    if (apos == bpos)
        return 0;
    return (apos > bpos) ? 1 : -1;
}

/*
 * Removes duplicate pos entries. If there's two entries with same pos
 * but different weight, the higher weight is retained.
 *
 * Returns new length.
 */
static int uniquePos(WordEntryPos* a, int l)
{
    WordEntryPos* ptr = NULL;
    WordEntryPos* res = NULL;

    if (l <= 1)
        return l;

    qsort((void*)a, l, sizeof(WordEntryPos), comparePos);

    res = a;
    ptr = a + 1;
    while (ptr - a < l) {
        if (WEP_GETPOS(*ptr) != WEP_GETPOS(*res)) {
            res++;
            *res = *ptr;
            if (res - a >= MAXNUMPOS - 1 || WEP_GETPOS(*res) == MAXENTRYPOS - 1)
                break;
        } else if (WEP_GETWEIGHT(*ptr) > WEP_GETWEIGHT(*res))
            WEP_SETWEIGHT(*res, WEP_GETWEIGHT(*ptr));
        ptr++;
    }

    return res + 1 - a;
}

/* Compare two WordEntryIN values for qsort */
static int compareentry(const void* va, const void* vb, void* arg)
{
    const WordEntryIN* a = (const WordEntryIN*)va;
    const WordEntryIN* b = (const WordEntryIN*)vb;
    char* BufferStr = (char*)arg;

    return tsCompareString(&BufferStr[a->entry.pos], a->entry.len, &BufferStr[b->entry.pos], b->entry.len, false);
}

/*
 * Sort an array of WordEntryIN, remove duplicates.
 * *outbuflen receives the amount of space needed for strings and positions.
 */
static int uniqueentry(WordEntryIN* a, int l, char* buf, int* outbuflen)
{
    int buflen;
    WordEntryIN *ptr = NULL, *res = NULL;
    errno_t rc;

    Assert(l >= 1);

    if (l > 1)
        qsort_arg((void*)a, l, sizeof(WordEntryIN), compareentry, (void*)buf);

    buflen = 0;
    res = a;
    ptr = a + 1;
    while (ptr - a < l) {
        if (!(ptr->entry.len == res->entry.len &&
                strncmp(&buf[ptr->entry.pos], &buf[res->entry.pos], res->entry.len) == 0)) {
            /* done accumulating data into *res, count space needed */
            buflen += res->entry.len;
            if (res->entry.haspos) {
                res->poslen = uniquePos(res->pos, res->poslen);
                buflen = SHORTALIGN(buflen);
                buflen += res->poslen * sizeof(WordEntryPos) + sizeof(uint16);
            }
            res++;
            if (res != ptr) {
                rc = memcpy_s(res, sizeof(WordEntryIN), ptr, sizeof(WordEntryIN));
                securec_check(rc, "\0", "\0");
            }
        } else if (ptr->entry.haspos) {
            if (res->entry.haspos) {
                /* append ptr's positions to res's positions */
                int newlen = ptr->poslen + res->poslen;

                res->pos = (WordEntryPos*)repalloc(res->pos, newlen * sizeof(WordEntryPos));

                rc = memcpy_s(&res->pos[res->poslen],
                    ptr->poslen * sizeof(WordEntryPos),
                    ptr->pos,
                    ptr->poslen * sizeof(WordEntryPos));
                securec_check(rc, "\0", "\0");

                res->poslen = newlen;
                pfree_ext(ptr->pos);
            } else {
                /* just give ptr's positions to pos */
                res->entry.haspos = 1;
                res->pos = ptr->pos;
                res->poslen = ptr->poslen;
            }
        }
        ptr++;
    }

    /* count space needed for last item */
    buflen += res->entry.len;
    if (res->entry.haspos) {
        res->poslen = uniquePos(res->pos, res->poslen);
        buflen = SHORTALIGN(buflen);
        buflen += res->poslen * sizeof(WordEntryPos) + sizeof(uint16);
    }

    *outbuflen = buflen;
    return res + 1 - a;
}

static int WordEntryCMP(WordEntry* a, WordEntry* b, char* buf)
{
    return compareentry(a, b, buf);
}

Datum tsvectorin(PG_FUNCTION_ARGS)
{
    char* buf = PG_GETARG_CSTRING(0);
    TSVectorParseState state;
    WordEntryIN* arr = NULL;
    int totallen;
    int arrlen; /* allocated size of arr */
    WordEntry* inarr = NULL;
    int len = 0;
    TSVector in;
    int i;
    char* token = NULL;
    int toklen;
    WordEntryPos* pos = NULL;
    int poslen;
    char* strbuf = NULL;
    int stroff;

    /*
     * Tokens are appended to tmpbuf, cur is a pointer to the end of used
     * space in tmpbuf.
     */
    char* tmpbuf = NULL;
    char* cur = NULL;
    int buflen = 256; /* allocated size of tmpbuf */

    state = init_tsvector_parser(buf, false, false);

    arrlen = 64;
    arr = (WordEntryIN*)palloc(sizeof(WordEntryIN) * arrlen);
    cur = tmpbuf = (char*)palloc(buflen);
    errno_t rc;

    while (gettoken_tsvector(state, &token, &toklen, &pos, &poslen, NULL)) {
        if (toklen >= MAXSTRLEN)
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("word is too long (%ld bytes, max %ld bytes)", (long)toklen, (long)(MAXSTRLEN - 1))));

        if (cur - tmpbuf > MAXSTRPOS)
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("string is too long for tsvector (%ld bytes, max %ld bytes)",
                        (long)(cur - tmpbuf),
                        (long)MAXSTRPOS)));

        /*
         * Enlarge buffers if needed
         */
        if (len >= arrlen) {
            arrlen *= 2;
            arr = (WordEntryIN*)repalloc((void*)arr, sizeof(WordEntryIN) * arrlen);
        }
        while ((cur - tmpbuf) + toklen >= buflen) {
            int dist = cur - tmpbuf;

            buflen *= 2;
            tmpbuf = (char*)repalloc((void*)tmpbuf, buflen);
            cur = tmpbuf + dist;
        }
        arr[len].entry.len = toklen;
        arr[len].entry.pos = cur - tmpbuf;
        rc = memcpy_s((void*)cur, (uint4)toklen, (void*)token, (uint4)toklen);
        securec_check(rc, "\0", "\0");

        cur += toklen;

        if (poslen != 0) {
            arr[len].entry.haspos = 1;
            arr[len].pos = pos;
            arr[len].poslen = poslen;
        } else {
            arr[len].entry.haspos = 0;
            arr[len].pos = NULL;
            arr[len].poslen = 0;
        }
        len++;
    }

    close_tsvector_parser(state);

    if (len > 0)
        len = uniqueentry(arr, len, tmpbuf, &buflen);
    else
        buflen = 0;

    if (buflen > MAXSTRPOS)
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("string is too long for tsvector (%d bytes, max %d bytes)", buflen, MAXSTRPOS)));

    totallen = CALCDATASIZE(len, buflen);
    in = (TSVector)palloc0(totallen);
    SET_VARSIZE(in, totallen);
    in->size = len;
    inarr = ARRPTR(in);
    strbuf = STRPTR(in);
    stroff = 0;
    int strbuf_len = totallen - (int)(strbuf - (char*)in);
    for (i = 0; i < len; i++) {
        if (arr[i].entry.len > 0) {
            rc = memcpy_s(strbuf + stroff, strbuf_len - stroff, &tmpbuf[arr[i].entry.pos], arr[i].entry.len);
            securec_check(rc, "\0", "\0");
        }
        arr[i].entry.pos = stroff;
        stroff += arr[i].entry.len;
        if (arr[i].entry.haspos) {
            if (arr[i].poslen > 0xFFFF)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("positions array too long")));

            /* Copy number of positions */
            stroff = SHORTALIGN(stroff);
            *(uint16*)(strbuf + stroff) = (uint16)arr[i].poslen;
            stroff += sizeof(uint16);

            /* Copy positions */
            if (arr[i].poslen > 0) {
                rc = memcpy_s(strbuf + stroff, strbuf_len - stroff, arr[i].pos, arr[i].poslen * sizeof(WordEntryPos));
                securec_check(rc, "\0", "\0");
            }
            stroff += arr[i].poslen * sizeof(WordEntryPos);

            pfree_ext(arr[i].pos);
        }
        inarr[i] = arr[i].entry;
    }

    Assert((strbuf + stroff - (char*)in) == totallen);

    PG_RETURN_TSVECTOR(in);
}

Datum tsvectorout(PG_FUNCTION_ARGS)
{
    TSVector out = PG_GETARG_TSVECTOR(0);
    char* outbuf = NULL;
    int4 i, lenbuf = 0, pp;
    WordEntry* ptr = ARRPTR(out);
    char* curbegin = NULL;
    char* curin = NULL;
    char* curout = NULL;

    lenbuf = out->size * 2 /* '' */ + out->size - 1 /* space */ + 2 /* \0 */;
    for (i = 0; i < out->size; i++) {
        lenbuf += ptr[i].len * 2 * pg_database_encoding_max_length() /* for escape */;
        if (ptr[i].haspos)
            lenbuf += 1 /* : */ + 7 /* int2 + , + weight */ * POSDATALEN(out, &(ptr[i]));
    }

    curout = outbuf = (char*)palloc(lenbuf);
    for (i = 0; i < out->size; i++) {
        curbegin = curin = STRPTR(out) + ptr->pos;
        if (i != 0)
            *curout++ = ' ';
        *curout++ = '\'';
        while (curin - curbegin < ptr->len) {
            int len = pg_mblen(curin);

            if (t_iseq(curin, '\''))
                *curout++ = '\'';
            else if (t_iseq(curin, '\\'))
                *curout++ = '\\';

            while (len--)
                *curout++ = *curin++;
        }

        *curout++ = '\'';
        if ((pp = POSDATALEN(out, ptr)) != 0) {

            int rc = 0;
            WordEntryPos* wptr = NULL;

            *curout++ = ':';
            wptr = POSDATAPTR(out, ptr);
            while (pp) {
                rc = sprintf_s(curout, lenbuf, "%d", WEP_GETPOS(*wptr));
                securec_check_ss_c(rc, "\0", "\0");
                curout = rc + curout;

                switch (WEP_GETWEIGHT(*wptr)) {
                    case 3:
                        *curout++ = 'A';
                        break;
                    case 2:
                        *curout++ = 'B';
                        break;
                    case 1:
                        *curout++ = 'C';
                        break;
                    case 0:
                    default:
                        break;
                }

                if (pp > 1)
                    *curout++ = ',';
                pp--;
                wptr++;
            }
        }
        ptr++;
    }

    *curout = '\0';
    PG_FREE_IF_COPY(out, 0);
    PG_RETURN_CSTRING(outbuf);
}

/*
 * Binary Input / Output functions. The binary format is as follows:
 *
 * uint32	number of lexemes
 *
 * for each lexeme:
 *		lexeme text in client encoding, null-terminated
 *		uint16	number of positions
 *		for each position:
 *			uint16 WordEntryPos
 */

Datum tsvectorsend(PG_FUNCTION_ARGS)
{
    TSVector vec = PG_GETARG_TSVECTOR(0);
    StringInfoData buf;
    int i, j;
    WordEntry* weptr = ARRPTR(vec);

    pq_begintypsend(&buf);

    pq_sendint32(&buf, vec->size);
    for (i = 0; i < vec->size; i++) {
        uint16 npos;

        /*
         * the strings in the TSVector array are not null-terminated, so we
         * have to send the null-terminator separately
         */
        pq_sendtext(&buf, STRPTR(vec) + weptr->pos, weptr->len);
        pq_sendbyte(&buf, '\0');

        npos = POSDATALEN(vec, weptr);
        pq_sendint16(&buf, npos);

        if (npos > 0) {
            WordEntryPos* wepptr = POSDATAPTR(vec, weptr);

            for (j = 0; j < npos; j++)
                pq_sendint16(&buf, wepptr[j]);
        }
        weptr++;
    }

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum tsvectorrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    TSVector vec;
    int i;
    int32 nentries;
    int datalen; /* number of bytes used in the variable size
                  * area after fixed size TSVector header and
                  * WordEntries */
    Size hdrlen;
    Size len; /* allocated size of vec */
    bool needSort = false;

    nentries = pq_getmsgint(buf, sizeof(int32));
    if (nentries < 0 || 
        (unsigned int)(nentries) > (((Size)(MaxAllocSize / 2) - (Size)DATAHDRSIZE) / sizeof(WordEntry))) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG), errmsg("invalid size of tsvector")));
    }

    hdrlen = DATAHDRSIZE + sizeof(WordEntry) * nentries;

    len = hdrlen * 2; /* times two to make room for lexemes */
    vec = (TSVector)palloc0(len);
    vec->size = nentries;

    datalen = 0;
    for (i = 0; i < nentries; i++) {
        const char* lexeme = NULL;
        uint16 npos;
        size_t lex_len;

        lexeme = pq_getmsgstring(buf);
        npos = (uint16)pq_getmsgint(buf, sizeof(uint16));

        /* sanity checks */

        lex_len = strlen(lexeme);
        if (lex_len > MAXSTRLEN)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("invalid tsvector: lexeme too long")));

        if (datalen > MAXSTRPOS)
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("invalid tsvector: maximum total lexeme length exceeded")));

        if (npos > MAXNUMPOS)
            ereport(
                ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("unexpected number of tsvector positions")));

        /*
         * Looks valid. Fill the WordEntry struct, and copy lexeme.
         *
         * But make sure the buffer is large enough first.
         */
        while (hdrlen + SHORTALIGN(datalen + lex_len) + (npos + 1) * sizeof(WordEntryPos) >= len) {
            len *= 2;
            vec = (TSVector)repalloc(vec, len);
        }

        vec->entries[i].haspos = (npos > 0) ? 1 : 0;
        vec->entries[i].len = lex_len;
        vec->entries[i].pos = datalen;

        int offset = STRPTR(vec) - (char*)vec;
        if (lex_len > 0) {
            errno_t rc = memcpy_s(STRPTR(vec) + datalen, len - offset - datalen, lexeme, lex_len);
            securec_check(rc, "\0", "\0");
        }

        datalen += lex_len;

        if (i > 0 && WordEntryCMP(&vec->entries[i], &vec->entries[i - 1], STRPTR(vec)) <= 0)
            needSort = true;

        /* Receive positions */
        if (npos > 0) {
            uint16 j;
            WordEntryPos* wepptr = NULL;

            /*
             * Pad to 2-byte alignment if necessary. Though we used palloc0
             * for the initial allocation, subsequent repalloc'd memory areas
             * are not initialized to zero.
             */
            if ((uintptr_t)datalen != SHORTALIGN(datalen)) {
                *(STRPTR(vec) + datalen) = '\0';
                datalen = SHORTALIGN(datalen);
            }

            int rc = memcpy_s(STRPTR(vec) + datalen, sizeof(uint16), &npos, sizeof(uint16));
            securec_check(rc, "\0", "\0");

            wepptr = POSDATAPTR(vec, &vec->entries[i]);
            for (j = 0; j < npos; j++) {
                wepptr[j] = (WordEntryPos)pq_getmsgint(buf, sizeof(WordEntryPos));
                if (j > 0 && WEP_GETPOS(wepptr[j]) <= WEP_GETPOS(wepptr[j - 1]))
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG), errmsg("position information is misordered")));
            }

            datalen += (npos + 1) * sizeof(WordEntry);
        }
    }

    SET_VARSIZE(vec, hdrlen + datalen);

    if (needSort)
        qsort_arg((void*)ARRPTR(vec), vec->size, sizeof(WordEntry), compareentry, (void*)STRPTR(vec));

    PG_RETURN_TSVECTOR(vec);
}
