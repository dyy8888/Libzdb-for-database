/*
 * Copyright (C) 2010-2013 Volodymyr Tarasenko <tvntsr@yahoo.com>
 *               2010      Sergey Pavlov <sergey.pavlov@gmail.com>
 *               2010      PortaOne Inc.
 * Copyright (C) Tildeslash Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and assDCIated documentation files (the "Software"), to deal
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
#include "Thread.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "OracleAdapter.h"

#include "StringBuffer.h"


/**
 * Implementation of the PreparedStatement/Delegate interface for oracle.
 *
 * @file
 */


/* ----------------------------------------------------------- Definitions */


typedef struct param_t {
        union {
                double real;
                long integer;
                const void *blob;
                const char *string;
                DCINumber number;
                DCIDateTime* date;
        } type;
        DCIInd is_null;
        int length;
        DCIBind* bind;
} *param_t;
#define T PreparedStatementDelegate_T
struct T {
        int         timeout;
        int         countdown;
        ub4         parameterCount;
        DCISession* usr;
        DCIStmt*    stmt;
        DCIEnv*     env;
        DCIError*   err;
        DCISvcCtx*  svc;
        param_t     params;
        sword       lastError;
        Thread_T    watchdog;
        char        running;
        ub4         rowsChanged;
        Connection_T delegator;
};
extern const struct Rop_T oraclerops;


/* --------------------------------------------------------- Private methods */


WATCHDOG(watchdog, T)


/* ------------------------------------------------------------- Constructor */


T OraclePreparedStatement_new(Connection_T delegator, DCIStmt *stmt, DCIEnv *env, DCISession* usr, DCIError *err, DCISvcCtx *svc) {
        T P;
        assert(stmt);
        assert(env);
        assert(err);
        assert(svc);
        NEW(P);
        P->delegator = delegator;
        P->stmt = stmt;
        P->env  = env;
        P->err  = err;
        P->svc  = svc;
        P->usr  = usr; 
        P->timeout = Connection_getQueryTimeout(P->delegator);
        P->lastError = DCI_SUCCESS;
        P->rowsChanged = 0;
        /* parameterCount */
        P->lastError = DCIAttrGet(P->stmt, DCI_HTYPE_STMT, &P->parameterCount, NULL, DCI_ATTR_BIND_COUNT, P->err);
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                P->parameterCount = 0;
        if (P->parameterCount)
                P->params = CALLOC(P->parameterCount, sizeof(struct param_t));
        P->running = false;
        if (P->timeout > 0) {
                Thread_create(P->watchdog, watchdog, P);
        }
        return P;
}


/* -------------------------------------------------------- Delegate Methods */


static void _free(T *P) {
	printf("偷偷执行了free\n");
        assert(P && *P);
        DCIHandleFree((*P)->stmt, DCI_HTYPE_STMT);
        if ((*P)->params) {
                // (*P)->params[i].bind is freed implicitly when the statement handle is deallocated
                FREE((*P)->params);
        }
        (*P)->svc = NULL;
        if ((*P)->watchdog)
                Thread_join((*P)->watchdog);
        FREE(*P);
}


static void _setString(T P, int parameterIndex, const char *x) {
        assert(P);
        int i = checkAndSetParameterIndex(parameterIndex, P->parameterCount);
        P->params[i].type.string = x;
        if (x) {
                P->params[i].length = (int)strlen(x);
                P->params[i].is_null = DCI_IND_NOTNULL;
        } else {
                P->params[i].length = 0;
                P->params[i].is_null = DCI_IND_NULL;
        }
       
        P->lastError = DCIBindByPos(P->stmt, &P->params[i].bind, P->err, parameterIndex, (char *)P->params[i].type.string,
                                    (int)P->params[i].length, SQLT_CHR, &P->params[i].is_null, 0, 0, 0, 0, DCI_DEFAULT);
         printf("在setstring中查看:%s,index:%d,绑定后的数组:%s\n",x,parameterIndex,(char *)P->params[i].type.string);
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
}


static void _setTimestamp(T P, int parameterIndex, time_t time) {
        assert(P);
        struct tm ts = {.tm_isdst = -1};
        ub4   valid;
        int i = checkAndSetParameterIndex(parameterIndex, P->parameterCount);

        P->lastError = DCIDescriptorAlloc((dvoid *)P->env, (dvoid **) &(P->params[i].type.date),
                                          (ub4) DCI_DTYPE_TIMESTAMP,
                                          (size_t) 0, (dvoid **) 0);
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));

        gmtime_r(&time, &ts);

        DCIDateTimeConstruct(P->usr,
                             P->err,
                             P->params[i].type.date, //DCIDateTime   *datetime,
                             ts.tm_year+1900, ts.tm_mon+1, ts.tm_mday, ts.tm_hour, ts.tm_min, ts.tm_sec, 0/*fsec*/,
                             (OraText*)0, 0);
        
        if (DCI_SUCCESS != DCIDateTimeCheck(P->usr, P->err, P->params[i].type.date, &valid) || valid != 0)
        {
                THROW(SQLException, "Invalid date/time value");
        }

        P->params[i].length = sizeof(DCIDateTime *);

        P->lastError = DCIBindByPos(P->stmt, &P->params[i].bind, P->err, parameterIndex, &P->params[i].type.date, 
                                    P->params[i].length, SQLT_TIMESTAMP, 0, 0, 0, 0, 0, DCI_DEFAULT);
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
}


static void _setInt(T P, int parameterIndex, int x) {
        assert(P);
        int i = checkAndSetParameterIndex(parameterIndex, P->parameterCount);
        P->params[i].type.integer = x;
        P->params[i].length = sizeof(x);
        P->lastError = DCIBindByPos(P->stmt, &P->params[i].bind, P->err, parameterIndex, &P->params[i].type.integer,
                                    (int)P->params[i].length, SQLT_INT, 0, 0, 0, 0, 0, DCI_DEFAULT);
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
}


static void _setLLong(T P, int parameterIndex, long long x) {
        assert(P);
        int i = checkAndSetParameterIndex(parameterIndex, P->parameterCount);
        P->params[i].length = sizeof(P->params[i].type.number);
        P->lastError = DCINumberFromInt(P->err, &x, sizeof(x), DCI_NUMBER_SIGNED, &P->params[i].type.number);
        if (P->lastError != DCI_SUCCESS)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
        P->lastError = DCIBindByPos(P->stmt, &P->params[i].bind, P->err, parameterIndex, &P->params[i].type.number, 
                                    (int)P->params[i].length, SQLT_VNU, 0, 0, 0, 0, 0, DCI_DEFAULT);
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
}


static void _setDouble(T P, int parameterIndex, double x) {
        assert(P);
        int i = checkAndSetParameterIndex(parameterIndex, P->parameterCount);
        P->params[i].type.real = x;
        P->params[i].length = sizeof(x);
        P->lastError = DCIBindByPos(P->stmt, &P->params[i].bind, P->err, parameterIndex, &P->params[i].type.real, 
                                    (int)P->params[i].length, SQLT_FLT, 0, 0, 0, 0, 0, DCI_DEFAULT);
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
}


static void _setBlob(T P, int parameterIndex, const void *x, int size) {
        assert(P);
        int i = checkAndSetParameterIndex(parameterIndex, P->parameterCount);
        P->params[i].type.blob = x;
        if (x) {
                P->params[i].length = size;
                P->params[i].is_null = DCI_IND_NOTNULL;
        } else {
                P->params[i].length = size;
                P->params[i].is_null = DCI_IND_NULL;
        }
        P->lastError = DCIBindByPos(P->stmt, &P->params[i].bind, P->err, parameterIndex, (void *)P->params[i].type.blob,
                                    (int)P->params[i].length, SQLT_BIN, NULL, NULL, NULL, NULL, NULL, DCI_DEFAULT);
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
}


static void _execute(T P) {
        assert(P);
        P->rowsChanged = 0;
        if (P->timeout > 0) {
                P->countdown = P->timeout;
                P->running = true;
        }
        printf("执行前查看长度:%d\n",P->parameterCount);
        for (int i=0;i<P->parameterCount;i++){
                printf("执行前查看参数:%d:%s\n",i,(char *)P->params[i].type.string);
        }
        P->lastError = DCIStmtExecute(P->svc, P->stmt, P->err, 1, 0, NULL, NULL, DCI_DEFAULT);
        P->running = false;
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
        P->lastError = DCIAttrGet( P->stmt, DCI_HTYPE_STMT, &P->rowsChanged, 0, DCI_ATTR_ROW_COUNT, P->err);
          printf("execute中查看返回码:%d\n",P->lastError);
        if (P->lastError != DCI_SUCCESS && P->lastError != DCI_SUCCESS_WITH_INFO)
                THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
}


static ResultSet_T _executeQuery(T P) {
        // printf("执行查询函数\n");
        assert(P);
        P->rowsChanged = 0;
        if (P->timeout > 0) {
                P->countdown = P->timeout;
                P->running = true;
        }
        P->lastError = DCIStmtExecute(P->svc, P->stmt, P->err, 0, 0, NULL, NULL, DCI_DEFAULT);
        P->running = false;
        if (P->lastError == DCI_SUCCESS || P->lastError == DCI_SUCCESS_WITH_INFO)
                return ResultSet_new(OracleResultSet_new(P->delegator, P->stmt, P->env, P->usr, P->err, P->svc, false), (Rop_T)&oraclerops);
        THROW(SQLException, "%s", OraclePreparedStatement_getLastError(P->lastError, P->err));
        return NULL;
}


static long long _rowsChanged(T P) {
        assert(P);
        return P->rowsChanged;
}


static int _parameterCount(T P) {
        assert(P);
        return P->parameterCount;
}


/* ------------------------------------------------------------------------- */


const struct Pop_T oraclepops = {
        .name           = "oracle",
        .free           = _free,
        .setString      = _setString,
        .setInt         = _setInt,
        .setLLong       = _setLLong,
        .setDouble      = _setDouble,
        .setTimestamp   = _setTimestamp,
        .setBlob        = _setBlob,
        .execute        = _execute,
        .executeQuery   = _executeQuery,
        .rowsChanged    = _rowsChanged,
        .parameterCount = _parameterCount
};

