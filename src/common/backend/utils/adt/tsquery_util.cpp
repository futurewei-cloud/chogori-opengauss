/* -------------------------------------------------------------------------
 *
 * tsquery_util.c
 *	  Utilities for tsquery datatype
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/tsquery_util.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "tsearch/ts_utils.h"
#include "miscadmin.h"

QTNode* QT2QTN(QueryItem* in, char* operand)
{
    QTNode* node = (QTNode*)palloc0(sizeof(QTNode));

    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    node->valnode = in;

    if (in->type == QI_OPR) {
        node->child = (QTNode**)palloc0(sizeof(QTNode*) * 2);
        node->child[0] = QT2QTN(in + 1, operand);
        node->sign = node->child[0]->sign;
        if (in->qoperator.oper == OP_NOT)
            node->nchild = 1;
        else {
            node->nchild = 2;
            node->child[1] = QT2QTN(in + in->qoperator.left, operand);
            node->sign |= node->child[1]->sign;
        }
    } else if (operand != NULL) {
        node->word = operand + in->qoperand.distance;
        node->sign = ((uint32)1) << (((unsigned int)in->qoperand.valcrc) % 32);
    }

    return node;
}

void QTNFree(QTNode* in)
{
    if (in == NULL)
        return;

    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (in->valnode && in->valnode->type == QI_VAL && in->word && (in->flags & QTN_WORDFREE) != 0)
        pfree_ext(in->word);

    if (in->child) {
        if (in->valnode) {
            if (in->valnode->type == QI_OPR && in->nchild > 0) {
                int i;

                for (i = 0; i < in->nchild; i++)
                    QTNFree(in->child[i]);
            }
            if (in->flags & QTN_NEEDFREE)
                pfree_ext(in->valnode);
        }
        pfree_ext(in->child);
    }

    pfree_ext(in);
}

int QTNodeCompare(QTNode* an, QTNode* bn)
{
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (an->valnode->type != bn->valnode->type)
        return (an->valnode->type > bn->valnode->type) ? -1 : 1;

    if (an->valnode->type == QI_OPR) {
        QueryOperator* ao = &an->valnode->qoperator;
        QueryOperator* bo = &bn->valnode->qoperator;

        if (ao->oper != bo->oper)
            return (ao->oper > bo->oper) ? -1 : 1;

        if (an->nchild != bn->nchild)
            return (an->nchild > bn->nchild) ? -1 : 1;

        {
            int i, res;

            for (i = 0; i < an->nchild; i++)
                if ((res = QTNodeCompare(an->child[i], bn->child[i])) != 0)
                    return res;
        }
        return 0;
    } else if (an->valnode->type == QI_VAL) {
        QueryOperand* ao = &an->valnode->qoperand;
        QueryOperand* bo = &bn->valnode->qoperand;

        if (ao->valcrc != bo->valcrc) {
            return (ao->valcrc > bo->valcrc) ? -1 : 1;
        }

        return tsCompareString(an->word, ao->length, bn->word, bo->length, false);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized QueryItem type: %d", an->valnode->type)));
        return 0; /* keep compiler quiet */
    }
}

static int cmpQTN(const void* a, const void* b)
{
    return QTNodeCompare(*(QTNode* const*)a, *(QTNode* const*)b);
}

void QTNSort(QTNode* in)
{
    int i;

    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (in->valnode->type != QI_OPR)
        return;

    for (i = 0; i < in->nchild; i++)
        QTNSort(in->child[i]);
    if (in->nchild > 1)
        qsort((void*)in->child, in->nchild, sizeof(QTNode*), cmpQTN);
}

bool QTNEq(QTNode* a, QTNode* b)
{
    uint32 sign = a->sign & b->sign;

    if (!(sign == a->sign && sign == b->sign))
        return 0;

    return (QTNodeCompare(a, b) == 0) ? true : false;
}

/*
 * Remove unnecessary intermediate nodes. For example:
 *
 *	OR			OR
 * a  OR	-> a b c
 *	 b	c
 */
void QTNTernary(QTNode* in)
{
    int i;
    errno_t rc;
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (in->valnode->type != QI_OPR)
        return;

    for (i = 0; i < in->nchild; i++)
        QTNTernary(in->child[i]);

    for (i = 0; i < in->nchild; i++) {
        QTNode* cc = in->child[i];

        if (cc->valnode->type == QI_OPR && in->valnode->qoperator.oper == cc->valnode->qoperator.oper) {
            int oldnchild = in->nchild;

            in->nchild += cc->nchild - 1;
            in->child = (QTNode**)repalloc(in->child, in->nchild * sizeof(QTNode*));

            if (i + 1 != oldnchild) {
                rc = memmove_s(in->child + i + cc->nchild,
                    (in->nchild - i - cc->nchild) * sizeof(QTNode*),
                    in->child + i + 1,
                    (oldnchild - i - 1) * sizeof(QTNode*));
                securec_check(rc, "\0", "\0");
            }

            rc = memcpy_s(
                in->child + i, (uint4)cc->nchild * sizeof(QTNode*), cc->child, (uint4)cc->nchild * sizeof(QTNode*));
            securec_check(rc, "\0", "\0");

            i += cc->nchild - 1;

            if (cc->flags & QTN_NEEDFREE)
                pfree_ext(cc->valnode);
            pfree_ext(cc);
        }
    }
}

/*
 * Convert a tree to binary tree by inserting intermediate nodes.
 * (Opposite of QTNTernary)
 */
void QTNBinary(QTNode* in)
{
    int i;

    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (in->valnode->type != QI_OPR)
        return;

    for (i = 0; i < in->nchild; i++)
        QTNBinary(in->child[i]);

    if (in->nchild <= 2)
        return;

    while (in->nchild > 2) {
        QTNode* nn = (QTNode*)palloc0(sizeof(QTNode));

        nn->valnode = (QueryItem*)palloc0(sizeof(QueryItem));
        nn->child = (QTNode**)palloc0(sizeof(QTNode*) * 2);

        nn->nchild = 2;
        nn->flags = QTN_NEEDFREE;

        nn->child[0] = in->child[0];
        nn->child[1] = in->child[1];
        nn->sign = nn->child[0]->sign | nn->child[1]->sign;

        nn->valnode->type = in->valnode->type;
        nn->valnode->qoperator.oper = in->valnode->qoperator.oper;

        in->child[0] = nn;
        in->child[1] = in->child[in->nchild - 1];
        in->nchild--;
    }
}

/*
 * Count the total length of operand string in tree, including '\0'-
 * terminators.
 */
static void cntsize(QTNode* in, int* sumlen, int* nnode)
{
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    *nnode += 1;
    if (in->valnode->type == QI_OPR) {
        int i;

        for (i = 0; i < in->nchild; i++)
            cntsize(in->child[i], sumlen, nnode);
    } else {
        *sumlen += in->valnode->qoperand.length + 1;
    }
}

typedef struct {
    QueryItem* curitem;
    char* operand;
    char* curoperand;
} QTN2QTState;

static void fillQT(QTN2QTState* state, QTNode* in, int state_curoperand_res_buffer_len)
{
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    errno_t rc;

    if (in->valnode->type == QI_VAL) {
        rc = memcpy_s(state->curitem, sizeof(QueryItem), in->valnode, sizeof(QueryOperand));
        securec_check(rc, "\0", "\0");

        if (in->valnode->qoperand.length > 0) {
            rc = memcpy_s(state->curoperand, state_curoperand_res_buffer_len, in->word, in->valnode->qoperand.length);
            securec_check(rc, "\0", "\0");
        }

        state->curitem->qoperand.distance = state->curoperand - state->operand;
        state->curoperand[in->valnode->qoperand.length] = '\0';
        state->curoperand += in->valnode->qoperand.length + 1;
        state_curoperand_res_buffer_len -= (in->valnode->qoperand.length + 1);
        state->curitem++;
    } else {
        QueryItem* curitem = state->curitem;

        Assert(in->valnode->type == QI_OPR);

        rc = memcpy_s(state->curitem, sizeof(QueryItem), in->valnode, sizeof(QueryOperator));
        securec_check(rc, "\0", "\0");

        Assert(in->nchild <= 2);
        state->curitem++;

        fillQT(state, in->child[0], state_curoperand_res_buffer_len);

        if (in->nchild == 2) {
            curitem->qoperator.left = state->curitem - curitem;
            fillQT(state, in->child[1], state_curoperand_res_buffer_len);
        }
    }
}

TSQuery QTN2QT(QTNode* in)
{
    TSQuery out;
    int len;
    int sumlen = 0, nnode = 0;
    QTN2QTState state;

    cntsize(in, &sumlen, &nnode);

    if (TSQUERY_TOO_BIG(nnode, sumlen))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("tsquery is too large")));
    len = COMPUTESIZE(nnode, sumlen);

    out = (TSQuery)palloc0(len);
    SET_VARSIZE(out, len);
    out->size = nnode;

    state.curitem = GETQUERY(out);
    state.operand = state.curoperand = GETOPERAND(out);
    int operand_len = len - (state.operand - (char*)out);

    fillQT(&state, in, operand_len);
    return out;
}

QTNode* QTNCopy(QTNode* in)
{
    QTNode* out = NULL;

    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    out = (QTNode*)palloc(sizeof(QTNode));

    *out = *in;
    out->valnode = (QueryItem*)palloc(sizeof(QueryItem));
    *(out->valnode) = *(in->valnode);
    out->flags |= QTN_NEEDFREE;

    if (in->valnode->type == QI_VAL) {
        out->word = (char*)palloc(in->valnode->qoperand.length + 1);
        errno_t rc = memcpy_s(out->word, in->valnode->qoperand.length, in->word, in->valnode->qoperand.length);
        securec_check(rc, "\0", "\0");
        out->word[in->valnode->qoperand.length] = '\0';
        out->flags |= QTN_WORDFREE;
    } else {
        int i;

        out->child = (QTNode**)palloc(sizeof(QTNode*) * in->nchild);

        for (i = 0; i < in->nchild; i++)
            out->child[i] = QTNCopy(in->child[i]);
    }

    return out;
}

void QTNClearFlags(QTNode* in, uint32 flags)
{
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    in->flags &= ~flags;

    if (in->valnode->type != QI_VAL) {
        int i;

        for (i = 0; i < in->nchild; i++)
            QTNClearFlags(in->child[i], flags);
    }
}
