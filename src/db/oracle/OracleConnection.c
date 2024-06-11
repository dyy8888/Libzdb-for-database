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
#include "Thread.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "OracleAdapter.h"
#include "StringBuffer.h"
#include "ConnectionDelegate.h"


/**
 * Implementation of the Connection/Delegate interface for oracle. 
 *
 * @file
 */


/* ----------------------------------------------------------- Definitions */


#define ERB_SIZE 152
#define ORACLE_TRANSACTION_PERIOD 10
#define T ConnectionDelegate_T
struct T {
        Connection_T   delegator;
        GCIEnv*        env;
        GCIError*      err;
        GCISvcCtx*     svc;
        GCISession*    usr;
        GCIServer*     srv;
        GCITrans*      txnhp;
        char           erb[ERB_SIZE];
        int            maxRows;
        int            timeout;
        int            countdown;
        sword          lastError;
        ub4            rowsChanged;
        StringBuffer_T sb;
        Thread_T       watchdog;
        char           running;
};
extern const struct Rop_T oraclerops;
extern const struct Pop_T oraclepops;


/* ------------------------------------------------------- Private methods */


static const char *_getErrorDescription(T C) {
        sb4 errcode;
        switch (C->lastError)
        {
                case GCI_SUCCESS:
                        return "";
                case GCI_SUCCESS_WITH_INFO:
                        return "Info - GCI_SUCCESS_WITH_INFO";
                        break;
                case GCI_NEED_DATA:
                        return "Error - GCI_NEED_DATA";
                        break;
                case GCI_NO_DATA:
                        return "Error - GCI_NODATA";
                        break;
                case GCI_ERROR:
                        (void) GCIErrorGet(C->err, 1, NULL, &errcode, C->erb, (ub4)ERB_SIZE, GCI_HTYPE_ERROR);
                        return C->erb;
                        break;
                case GCI_INVALID_HANDLE:
                        return "Error - GCI_INVALID_HANDLE";
                        break;
                case GCI_STILL_EXECUTING:
                        return "Error - GCI_STILL_EXECUTE";
                        break;
                case GCI_CONTINUE:
                        return "Error - GCI_CONTINUE";
                        break;
                default:
                        break;
        }
        return C->erb;
}


static bool _doConnect(T C, char**  error) {
#define ERROR(e) do {*error = Str_dup(e); return false;} while (0)
#define ORAERROR(e) do{ *error = Str_dup(_getErrorDescription(e)); return false;} while(0)
        URL_T url = Connection_getURL(C->delegator);
        const char *servicename, *username, *password;
        const char *host = URL_getHost(url);
        int port = URL_getPort(url);
        if (! (username = URL_getUser(url)))
                if (! (username = URL_getParameter(url, "user")))
                        ERROR("no username specified in URL");
        if (! (password = URL_getPassword(url)))
                if (! (password = URL_getParameter(url, "password")))
                        ERROR("no password specified in URL");
        if (! (servicename = URL_getPath(url)))
                ERROR("no Service Name specified in URL");
        ++servicename;
        dvoid *tmp;
        GCIInitialize((ub4) GCI_THREADED | GCI_OBJECT, (dvoid *)0, (dvoid * (*)(void*, size_t)) 0, (dvoid * (*)(void*, void*, size_t)) 0,  (void (*)(void*, void*)) 0 );    
        GCIHandleAlloc((dvoid *) NULL, (dvoid **)&C->env, (ub4)GCI_HTYPE_ENV, 52, (dvoid **)&tmp);
        GCIEnvInit(&C->env, (ub4)GCI_DEFAULT, 21, (dvoid **)&tmp);     
        GCIHandleAlloc((dvoid *)C->env, (dvoid **)&C->err, (ub4)GCI_HTYPE_ERROR, 52, (dvoid **)&tmp);      
        // GCIHandleAlloc( (dvoid *) C->env, (dvoid **) &C->stmt , (ub4) GCI_HTYPE_STMT, 50, (dvoid **)&tmp);
        StringBuffer_clear(C->sb);
        C->lastError = GCILogon(C->env,C->err, &C->svc ,username, (ub4)strlen((char*)username), password, (ub4)strlen((char*)password), servicename, (ub4)strlen((char*)servicename));
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO)
                ORAERROR(C); 
        return true;
}


WATCHDOG(watchdog, T)


/* -------------------------------------------------------- Delegate Methods */


static const char *_getLastError(T C) {
        return _getErrorDescription(C);
}

static void _free(T* C) {
        assert(C && *C);
        if ((*C)->svc) {
                GCISessionEnd((*C)->svc, (*C)->err, (*C)->usr, GCI_DEFAULT);
                (*C)->svc = NULL;
        }
        if ((*C)->srv)
                GCIServerDetach((*C)->srv, (*C)->err, GCI_DEFAULT);
        if ((*C)->env)
                GCIHandleFree((*C)->env, GCI_HTYPE_ENV);
        StringBuffer_free(&((*C)->sb));
        if ((*C)->watchdog)
            Thread_join((*C)->watchdog);
        FREE(*C);
}


static T _new(Connection_T delegator, char **error) {
        T C;
        assert(delegator);
        assert(error);
        NEW(C);
        C->delegator = delegator;
        C->sb = StringBuffer_create(STRLEN);
        if (! _doConnect(C, error)) {
                _free(&C);
                return NULL;
        }
        C->txnhp = NULL;
        C->running = false;
        return C;
}


static bool _ping(T C) {
        assert(C);
        C->lastError = GCIPing(C->svc, C->err, GCI_DEFAULT);
        return (C->lastError == GCI_SUCCESS);
}


static void _setQueryTimeout(T C, int ms) {
        assert(C);
        assert(ms >= 0);
        C->timeout = ms;
        if (ms > 0) {
                if (!C->watchdog) {
                        Thread_create(C->watchdog, watchdog, C);
                }
        } else {
                if (C->watchdog) {
                        GCISvcCtx* t = C->svc;
                        C->svc = NULL;
                        Thread_join(C->watchdog);
                        C->svc = t;
                        C->watchdog = 0;
                }
        }
}


static bool _beginTransaction(T C) {
        assert(C);
        // if (C->txnhp == NULL) /* Allocate handler only once, if it is necessary */
        // {
                // printf("2222222\n");
            /* allocate transaction handle and set it in the service handle */
        //     C->lastError = GCIHandleAlloc(C->env, (void **)&C->txnhp, GCI_HTYPE_TRANS, 0, 0);
        //     if (C->lastError != GCI_SUCCESS) 
                // return false;
        //     GCIAttrSet(C->svc, GCI_HTYPE_SVCCTX, (void *)C->txnhp, 0, GCI_ATTR_TRANS, C->err);
        // }
        C->lastError = GCITransStart (C->svc, C->err, 60, GCI_TRANS_NEW);
        // if (C->lastError!=GCI_SUCCESS)
        
        return true;
}


static bool _commit(T C) {
        assert(C);
        C->lastError = GCITransCommit(C->svc, C->err, (ub4)0);
        return C->lastError == GCI_SUCCESS;
}


static bool _rollback(T C) {
        assert(C);
        C->lastError = GCITransRollback(C->svc, C->err, (ub4)0);
        return C->lastError == GCI_SUCCESS;
}


static long long _lastRowId(T C) {
        /*:FIXME:*/
        /*
         Oracle's RowID can be mapped on string only
         so, currently I leave it unimplemented
         */
        
        /*     GCIRowid* rowid; */
        /*     GCIDescriptorAlloc((dvoid *)C->env,  */
        /*                        (dvoid **)&rowid, */
        /*                        (ub4) GCI_DTYPE_ROWID,  */
        /*                        (size_t) 0, (dvoid **) 0); */
        
        /*     if (GCIAttrGet (select_p, */
        /*                     GCI_HTYPE_STMT, */
        /*                     &rowid,              /\* get the current rowid *\/ */
        /*                     0, */
        /*                     GCI_ATTR_ROWID, */
        /*                     errhp)) */
        /*     { */
        /*         printf ("Getting the Rowid failed \n"); */
        /*         return (GCI_ERROR); */
        /*     } */
        
        /*     GCIDescriptorFree(rowid, GCI_DTYPE_ROWID); */
        DEBUG("OracleConnection_lastRowId: Not implemented yet");
        return -1;
}


static long long _rowsChanged(T C) {
        assert(C);
        return C->rowsChanged;
}


static bool _execute(T C, const char *sql, va_list ap) {
        GCIStmt* stmtp;
        va_list ap_copy;
        assert(C);
        C->rowsChanged = 0;
        va_copy(ap_copy, ap);
        StringBuffer_vset(C->sb, sql, ap_copy);
        va_end(ap_copy);
        StringBuffer_trim(C->sb);
        /* Build statement */
        C->lastError = GCIHandleAlloc(C->env, (void **)&stmtp, GCI_HTYPE_STMT, 0, NULL);
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO)
                return false;
        C->lastError = GCIStmtPrepareWithSvc(C->svc ,stmtp, C->err, StringBuffer_toString(C->sb), StringBuffer_length(C->sb), GCI_NTV_SYNTAX, GCI_DEFAULT);
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO) {
                GCIHandleFree(stmtp, GCI_HTYPE_STMT);
                return false;
        }
        /* Execute */
        if (C->timeout > 0) {
                C->countdown = C->timeout;
                C->running = true;
        }
        C->lastError = GCIStmtExecute(C->svc, stmtp, C->err, 1, 0, NULL, NULL, GCI_DEFAULT);
        C->running = false;
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO) {
                // ub4 parmcnt = 0;
                // // GCIAttrGet(stmtp, GCI_HTYPE_STMT, &parmcnt, NULL, GCI_ATTR_PARSE_ERROR_OFFSET, C->err);
                // DEBUG("Error occured in StmtExecute %d (%s), offset is %d\n", C->lastError, _getLastError(C), parmcnt);
                GCIHandleFree(stmtp, GCI_HTYPE_STMT);
                return false;
        }
        C->lastError = GCIAttrGet(stmtp, GCI_HTYPE_STMT, &C->rowsChanged, 0, GCI_ATTR_ROW_COUNT, C->err);
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO)
                DEBUG("OracleConnection_execute: Error in GCIAttrGet %d (%s)\n", C->lastError, _getLastError(C));
        GCIHandleFree(stmtp, GCI_HTYPE_STMT);
        return C->lastError == GCI_SUCCESS;
}


static ResultSet_T _executeQuery(T C, const char *sql, va_list ap) {
        GCIStmt* stmtp;
        va_list  ap_copy;
        assert(C);
        C->rowsChanged = 0;
        va_copy(ap_copy, ap);
        StringBuffer_vset(C->sb, sql, ap_copy);
        va_end(ap_copy);
        StringBuffer_trim(C->sb);
        /* Build statement */
        C->lastError = GCIHandleAlloc(C->env, (void **)&stmtp, GCI_HTYPE_STMT, 0, NULL);
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO)
                return NULL;
        C->lastError = GCIStmtPrepareWithSvc(C->svc ,stmtp, C->err, StringBuffer_toString(C->sb), StringBuffer_length(C->sb), GCI_NTV_SYNTAX, GCI_DEFAULT);
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO) {
                GCIHandleFree(stmtp, GCI_HTYPE_STMT);
                return NULL;
        }
        /* Execute and create Result Set */
        if (C->timeout > 0) {
                C->countdown = C->timeout;
                C->running = true;
        }
        C->lastError = GCIStmtExecute(C->svc, stmtp, C->err, 0, 0, NULL, NULL, GCI_DEFAULT);
        C->running = false;
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO) {
                ub4 parmcnt = 0;
                // GCIAttrGet(stmtp, GCI_HTYPE_STMT, &parmcnt, NULL, GCI_ATTR_PARSE_ERROR_OFFSET, C->err);
                DEBUG("Error occured in StmtExecute %d (%s), offset is %d\n", C->lastError, _getLastError(C), parmcnt);
                GCIHandleFree(stmtp, GCI_HTYPE_STMT);
                return NULL;
        }
        C->lastError = GCIAttrGet(stmtp, GCI_HTYPE_STMT, &C->rowsChanged, 0, GCI_ATTR_ROW_COUNT, C->err);
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO)
                DEBUG("OracleConnection_execute: Error in GCIAttrGet %d (%s)\n", C->lastError, _getLastError(C));
        return ResultSet_new(OracleResultSet_new(C->delegator, stmtp, C->env, C->usr, C->err, C->svc, true), (Rop_T)&oraclerops);
}


static PreparedStatement_T _prepareStatement(T C, const char *sql, va_list ap) {
        GCIStmt *stmtp;
        va_list ap_copy;
        assert(C);
        va_copy(ap_copy, ap);
        StringBuffer_vset(C->sb, sql, ap_copy);
        va_end(ap_copy);
        StringBuffer_trim(C->sb);
        int n=StringBuffer_prepare4oracle(C->sb);
        /* Build statement */
        
        C->lastError = GCIHandleAlloc(C->env, (void **)&stmtp, GCI_HTYPE_STMT, 0, 0);
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO)
        {       
                return NULL;
        }
        C->lastError = GCIStmtPrepareWithSvc(C->svc , stmtp, C->err, StringBuffer_toString(C->sb), StringBuffer_length(C->sb)+1, GCI_NTV_SYNTAX, GCI_DEFAULT);
        if (C->lastError != GCI_SUCCESS && C->lastError != GCI_SUCCESS_WITH_INFO) {
                GCIHandleFree(stmtp, GCI_HTYPE_STMT);
                return NULL;
        }
        return PreparedStatement_new(OraclePreparedStatement_new(C->delegator, stmtp, C->env, C->usr, C->err, C->svc,n), (Pop_T)&oraclepops);
}


/* ------------------------------------------------------------------------- */


const struct Cop_T oraclesqlcops = {
        .name             = "oracle",
        .new              = _new,
        .free             = _free,
        .ping             = _ping,
        .setQueryTimeout  = _setQueryTimeout,
        .beginTransaction = _beginTransaction,
        .commit           = _commit,
        .rollback         = _rollback,
        .lastRowId        = _lastRowId,
        .rowsChanged      = _rowsChanged,
        .execute          = _execute,
        .executeQuery     = _executeQuery,
        .prepareStatement = _prepareStatement,
        .getLastError     = _getLastError
};
