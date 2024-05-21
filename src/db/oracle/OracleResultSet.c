/*
 * Copyright (C) 2010-2013 Volodymyr Tarasenko <tvntsr@yahoo.com>
 *               2010      Sergey Pavlov <sergey.pavlov@gmail.com>
 *               2010      PortaOne Inc.
 * Copyright (C) Tildeslash Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "Config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "OracleAdapter.h"
#include "StringBuffer.h"


/**
 * Implementation of the ResulSet/Delegate interface for oracle.
 *
 * @file
 */


/* ----------------------------------------------------------- Definitions */


typedef struct column_t {
        DCIDefine *def;
        int isNull;
        char *buffer;
        char *name;
        unsigned long length;
        DCILobLocator *lob_loc;
        DCIDateTime   *date;
} *column_t;
#define T ResultSetDelegate_T
struct T {
        int         columnCount;
        int         currentRow;
        int         fetchSize;
        ub4         maxRows;
        DCIStmt*    stmt;
        DCIEnv*     env;
        DCISession* usr;
        DCIError*   err;
        DCISvcCtx*  svc;
        column_t    columns;
        sword       lastError;
        int         freeStatement;
        Connection_T delegator;
};
#ifndef ORACLE_COLUMN_NAME_LOWERCASE
#define ORACLE_COLUMN_NAME_LOWERCASE 2
#endif
#define LOB_CHUNK_SIZE  2000
#define DATE_STR_BUF_SIZE   255


/* ------------------------------------------------------- Private methods */


static bool _initaleDefiningBuffers(T R) {
        ub2 dtype = 0;
        int deptlen;
        DciText *name;
        ub4 len;
        int sizelen = sizeof(deptlen);
        DCIParam *pard = (DCIParam *) 0;
        sword status;
        for (int i = 1; i <= R->columnCount; i++) {
                deptlen = 0;
                /* The next two statements describe the select-list item, dname, and
                 return its length */
                R->lastError = DCIParamGet((void *)R->stmt, DCI_HTYPE_STMT, R->err, (void **)&pard, i);
                R->lastError = DCIAttrGet((void *)pard,DCI_DTYPE_PARAM,(void *)&name,(ub4 *)&len,DCI_ATTR_NAME,R->err);
                if (R->lastError != DCI_SUCCESS)
                        return false;
                R->lastError = DCIAttrGet(pard, DCI_DTYPE_PARAM, &deptlen, &sizelen, DCI_ATTR_DATA_SIZE, R->err);
                if (R->lastError != DCI_SUCCESS) {
                        // cannot get column's size, cleaning and returning
                        DCIDescriptorFree(pard, DCI_DTYPE_PARAM);
                        return false;
                }
                DCIAttrGet(pard, DCI_DTYPE_PARAM,(void *)&dtype, (ub4 *)&len, DCI_ATTR_DATA_TYPE, R->err);
                if (R->lastError != DCI_SUCCESS) {
                        printf("error\n");
                }
                /* Use the retrieved length of dname to allocate an output buffer, and
                 then define the output variable. */
                deptlen +=1;
                R->columns[i-1].length = deptlen;
                R->columns[i-1].isNull = 0;
                switch(dtype)
                {
                        case SQLT_BIN:
                                R->columns[i-1].buffer = NULL;
                                status = DCIDescriptorAlloc((dvoid *)R->env, (dvoid **) &(R->columns[i-1].lob_loc),
                                                            (ub4) DCI_DTYPE_LOB,
                                                            (size_t) 0, NULL);
                                R->lastError = DCIDefineByPos(R->stmt, &R->columns[i-1].def, R->err, i,
                                                              &(R->columns[i-1].lob_loc), deptlen, SQLT_BLOB, &(R->columns[i-1].isNull), 0, 0, DCI_DEFAULT);
                                break;
                                
                        case SQLT_CLOB:
                                R->columns[i-1].buffer = NULL;
                                status = DCIDescriptorAlloc((dvoid *)R->env, (dvoid **) &(R->columns[i-1].lob_loc),
                                                            (ub4) DCI_DTYPE_LOB,
                                                            (size_t) 0, (dvoid **) 0);
                                R->lastError = DCIDefineByPos(R->stmt, &R->columns[i-1].def, R->err, i,
                                                              &(R->columns[i-1].lob_loc), deptlen, SQLT_CLOB, &(R->columns[i-1].isNull), 0, 0, DCI_DEFAULT);
                                break;
                        case SQLT_DAT:
                        case SQLT_DATE:
                        case SQLT_TIMESTAMP:
                        case SQLT_TIMESTAMP_TZ:
                        case SQLT_TIMESTAMP_LTZ:
                                R->columns[i-1].buffer = NULL;
                                status = DCIDescriptorAlloc((dvoid *)R->env, (dvoid **) &(R->columns[i-1].date),
                                                            (ub4) DCI_DTYPE_TIMESTAMP,
                                                            (size_t) 0, (dvoid **) 0);
                                R->lastError = DCIDefineByPos(R->stmt, &R->columns[i-1].def, R->err, i,
                                                              &(R->columns[i-1].date), sizeof(R->columns[i-1].date), SQLT_TIMESTAMP, &(R->columns[i-1].isNull), 0, 0, DCI_DEFAULT);
                                break;
                        default:
                                R->columns[i-1].lob_loc = NULL;
                                R->columns[i-1].buffer = ALLOC(deptlen + 1);
                                R->lastError = DCIDefineByPos(R->stmt, &R->columns[i-1].def, R->err, i,
                                                              R->columns[i-1].buffer, deptlen, SQLT_STR, &(R->columns[i-1].isNull), 0, 0, DCI_DEFAULT);
                                
                }
                {
                        char *col_name;
                        ub4   col_name_len;
                        char* tmp_buffer;
                        
                        R->lastError = DCIAttrGet(pard, DCI_DTYPE_PARAM, &col_name, &col_name_len, DCI_ATTR_NAME, R->err);
                        if (R->lastError != DCI_SUCCESS)
                                continue;
                        // column name could be non NULL terminated
                        // it is not allowed to do: col_name[col_name_len] = 0;
                        // so, copy the string
                        tmp_buffer = Str_ndup(col_name, col_name_len);
// #if defined(ORACLE_COLUMN_NAME_LOWERCASE) && ORACLE_COLUMN_NAME_LOWERCASE > 1
                        // R->columns[i-1].name = CALLOC(1, col_name_len+1);
                        // DCIMultiByteStrCaseConversion(R->env, R->columns[i-1].name, tmp_buffer, DCI_NLS_LOWERCASE);
                        // FREE(tmp_buffer);
// #else
                        R->columns[i-1].name = tmp_buffer;
// #endif /*COLLUMN_NAME_LOWERCASE*/
                }
                DCIDescriptorFree(pard, DCI_DTYPE_PARAM);
                if (R->lastError != DCI_SUCCESS) {
                        return false;
                }
        }
        
        return true;
}

static bool _toString(T R, int i)
{
        const char fmt[] = "IYYY-MM-DD HH24.MI.SS"; // "YYYY-MM-DD HH24:MI:SS TZR TZD"
        
        R->columns[i].length = DATE_STR_BUF_SIZE;
        if (R->columns[i].buffer)
                FREE(R->columns[i].buffer);
        
        R->columns[i].buffer = ALLOC(R->columns[i].length + 1);
        R->lastError = DCIDateTimeToText(R->usr,
                                         R->err,
                                         R->columns[i].date,
                                         fmt, strlen(fmt),
                                         0,
                                         NULL, 0,
                                         (ub4*)&(R->columns[i].length), (DciText *)R->columns[i].buffer);
        return ((R->lastError == DCI_SUCCESS) || (R->lastError == DCI_SUCCESS_WITH_INFO));;
}

static void _setFetchSize(T R, int rows);


/* ------------------------------------------------------------- Constructor */


T OracleResultSet_new(Connection_T delegator, DCIStmt *stmt, DCIEnv *env, DCISession* usr, DCIError *err, DCISvcCtx *svc, int need_free) {
        T R;
        assert(stmt);
        assert(env);
        assert(err);
        assert(svc);
        NEW(R);
        R->delegator = delegator;
        R->stmt = stmt;
        R->env  = env;
        R->err  = err;
        R->svc  = svc;
        R->usr  = usr;
        R->maxRows = Connection_getMaxRows(R->delegator);
        R->freeStatement = need_free;
        /* Get the number of columns in the select list */
        R->lastError = DCIAttrGet (R->stmt, DCI_HTYPE_STMT, &R->columnCount, NULL, DCI_ATTR_PARAM_COUNT, R->err);
        if (R->lastError != DCI_SUCCESS && R->lastError != DCI_SUCCESS_WITH_INFO)
                DEBUG("_new: Error %d, '%s'\n", R->lastError, OraclePreparedStatement_getLastError(R->lastError,R->err));
        R->columns = CALLOC(R->columnCount, sizeof (struct column_t));
        if (!_initaleDefiningBuffers(R)) {
                DEBUG("_new: Error %d, '%s'\n", R->lastError, OraclePreparedStatement_getLastError(R->lastError,R->err));
                R->currentRow = -1;
        }
        if (R->currentRow != -1) {
                _setFetchSize(R, Connection_getFetchSize(R->delegator));
        }
        return R;
}


/* -------------------------------------------------------- Delegate Methods */


static void _free(T *R) {
        assert(R && *R);
        if ((*R)->freeStatement)
                DCIHandleFree((*R)->stmt, DCI_HTYPE_STMT);
        for (int i = 0; i < (*R)->columnCount; i++) {
                if ((*R)->columns[i].lob_loc)
                        DCIDescriptorFree((*R)->columns[i].lob_loc, DCI_DTYPE_LOB);
                if ((*R)->columns[i].date)
                        DCIDescriptorFree((dvoid*)(*R)->columns[i].date, DCI_DTYPE_TIMESTAMP);
                FREE((*R)->columns[i].buffer);
                FREE((*R)->columns[i].name);
        }
        FREE((*R)->columns);
        FREE(*R);
}


static int _getColumnCount(T R) {
        assert(R);
        return R->columnCount;
}


static const char *_getColumnName(T R, int column) {
        assert(R);
        if (R->columnCount < column)
                return NULL;
        return  R->columns[column-1].name;
}


static long _getColumnSize(T R, int columnIndex) {
        DCIParam* pard = NULL;
        ub4 char_semantics = 0;
        sb4 status;
        ub2 col_width = 0;
        assert(R);
        status = DCIParamGet(R->stmt, DCI_HTYPE_STMT, R->err, (void **)&pard, columnIndex);
        if (status != DCI_SUCCESS)
                return -1;
        status = DCIAttrGet(pard, DCI_DTYPE_PARAM, &char_semantics, NULL, DCI_ATTR_CHAR_USED, R->err);
        if (status != DCI_SUCCESS) {
                DCIDescriptorFree(pard, DCI_DTYPE_PARAM);
                return -1;
        }
        status = (char_semantics) ?
        /* Retrieve the column width in characters */
        DCIAttrGet(pard, DCI_DTYPE_PARAM, &col_width, NULL, DCI_ATTR_CHAR_SIZE, R->err) :
        /* Retrieve the column width in bytes */
        DCIAttrGet(pard, DCI_DTYPE_PARAM, &col_width, NULL, DCI_ATTR_DATA_SIZE, R->err);
        return (status != DCI_SUCCESS) ? -1 : col_width;
}


static void _setFetchSize(T R, int rows) {
        assert(R);
        assert(rows > 0);
        R->lastError = DCIAttrSet(R->stmt, DCI_HTYPE_STMT, (void*)&rows, (ub4)sizeof(ub4), DCI_ATTR_PREFETCH_ROWS, R->err);
        if (R->lastError != DCI_SUCCESS)
                DEBUG("DCIAttrSet -- %s\n", OraclePreparedStatement_getLastError(R->lastError, R->err));
        R->fetchSize = rows;
}


static int _getFetchSize(T R) {
        assert(R);
        return R->fetchSize;
}


static bool _next(T R) {
        assert(R);
        if ((R->currentRow < 0) || ((R->maxRows > 0) && (R->currentRow >= R->maxRows)))
                return false;
        R->lastError = DCIStmtFetch2(R->stmt, R->err, 1, DCI_FETCH_NEXT, 0, DCI_DEFAULT);
        if (R->lastError == DCI_NO_DATA)
                return false;
        if (R->lastError != DCI_SUCCESS && R->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(R->lastError, R->err));
        if (R->lastError == DCI_SUCCESS_WITH_INFO)
                DEBUG("_next Error %d, '%s'\n", R->lastError, OraclePreparedStatement_getLastError(R->lastError, R->err));
        R->currentRow++;
        return ((R->lastError == DCI_SUCCESS) || (R->lastError == DCI_SUCCESS_WITH_INFO));
}


static bool _isnull(T R, int columnIndex) {
        assert(R);
        int i = checkAndSetColumnIndex(columnIndex, R->columnCount);
        return R->columns[i].isNull != 0;
}


static const char *_getString(T R, int columnIndex) {
        assert(R);
        
        int i = checkAndSetColumnIndex(columnIndex, R->columnCount);
        printf("null number is %d and i is %d\n",R->columns[i].isNull,i);
        // if (R->columns[i].isNull)
        // {       
        //         printf("is null\n");
        //          return NULL;
        // }
               
        if (R->columns[i].date) {
                if (!_toString(R, i)) {
                        THROW(SQLException, "%s", OraclePreparedStatement_getLastError(R->lastError, R->err));
                }
        }
        if (R->columns[i].buffer)
        {
                printf("try to get string\n");
                 R->columns[i].buffer[R->columns[i].length] = 0;
        }
               
        return R->columns[i].buffer;
}


static const void *_getBlob(T R, int columnIndex, int *size) {
        printf("get blob\n");
        assert(R);
        int i = checkAndSetColumnIndex(columnIndex, R->columnCount);
        if (R->columns[i].isNull)
                return NULL;
        if (R->columns[i].buffer)
                FREE(R->columns[i].buffer);
        ub8 read_chars = 0;
        ub8 read_bytes = 0;
        ub8 total_bytes = 0;
        R->columns[i].buffer = ALLOC(LOB_CHUNK_SIZE);
        *size = 0;
        ub1 piece = DCI_FIRST_PIECE;
        do {
                read_bytes = 0;
                read_chars = 0;
                R->lastError = DCILobRead(R->svc, R->err, R->columns[i].lob_loc, &read_bytes, 1,
                                           R->columns[i].buffer + total_bytes, LOB_CHUNK_SIZE, (dvoid *) 0,
						0, (ub2) 0, (ub1) SQLCS_IMPLICIT);
                if (read_bytes) {
                        total_bytes += read_bytes;
                        piece = DCI_NEXT_PIECE;
                        R->columns[i].buffer = RESIZE(R->columns[i].buffer, (long)(total_bytes + LOB_CHUNK_SIZE));
                }
        } while (R->lastError == DCI_NEED_DATA);
        printf("test get clob\n");
        if (R->lastError != DCI_SUCCESS && R->lastError != DCI_SUCCESS_WITH_INFO) {
                FREE(R->columns[i].buffer);
                R->columns[i].buffer = NULL;
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(R->lastError, R->err));
        }
        *size = R->columns[i].length = (int)total_bytes;
        return (const void *)R->columns[i].buffer;
}


/* ------------------------------------------------------------------------- */


const struct Rop_T oraclerops = {
        .name           = "oracle",
        .free           = _free,
        .getColumnCount = _getColumnCount,
        .getColumnName  = _getColumnName,
        .getColumnSize  = _getColumnSize,
        .setFetchSize   = _setFetchSize,
        .getFetchSize   = _getFetchSize,
        .next           = _next,
        .isnull         = _isnull,
        .getString      = _getString,
        .getBlob        = _getBlob
        // getTimestamp and getDateTime is handled in ResultSet
};

