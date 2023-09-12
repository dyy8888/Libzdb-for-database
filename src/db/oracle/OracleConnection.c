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
        DCIEnv*        env;
        DCIError*      err;
        DCISvcCtx*     svc;
        DCISession*    usr;
        DCIServer*     srv;
        DCITrans*      txnhp;
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
                case DCI_SUCCESS:
                        return "";
                case DCI_SUCCESS_WITH_INFO:
                        return "Info - DCI_SUCCESS_WITH_INFO";
                        break;
                case DCI_NEED_DATA:
                        return "Error - DCI_NEED_DATA";
                        break;
                case DCI_NO_DATA:
                        return "Error - DCI_NODATA";
                        break;
                case DCI_ERROR:
                        (void) DCIErrorGet(C->err, 1, NULL, &errcode, C->erb, (ub4)ERB_SIZE, DCI_HTYPE_ERROR);
                        return C->erb;
                        break;
                case DCI_INVALID_HANDLE:
                        return "Error - DCI_INVALID_HANDLE";
                        break;
                case DCI_STILL_EXECUTING:
                        return "Error - DCI_STILL_EXECUTE";
                        break;
                case DCI_CONTINUE:
                        return "Error - DCI_CONTINUE";
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
        // printf("servicename:%s,username:%s,password:%s\n",servicename,username,password);
        /* Create a thread-safe DCI environment with N' substitution turned on. */
        if (DCIEnvCreate(&C->env, DCI_THREADED | DCI_OBJECT | DCI_NCHAR_LITERAL_REPLACE_ON, 0, 0, 0, 0, 0, 0))
        {
                ERROR("Create a DCI environment failed");
        }   
        /* allocate an error handle */
        if (DCI_SUCCESS != DCIHandleAlloc(C->env, (dvoid**)&C->err, DCI_HTYPE_ERROR, 0, 0))
        {
                ERROR("Allocating error handler failed");
        }      
        /* server contexts */
        if (DCI_SUCCESS != DCIHandleAlloc(C->env, (dvoid**)&C->srv, DCI_HTYPE_SERVER, 0, 0))
        {
                ERROR("Allocating server context failed");
        }
               
        /* allocate a service handle */
        if (DCI_SUCCESS != DCIHandleAlloc(C->env, (dvoid**)&C->svc, DCI_HTYPE_SVCCTX, 0, 0))
        {
                ERROR("Allocating service handle failed");
        }
        StringBuffer_clear(C->sb);
        /* Oracle connect string is on the form: //host[:port]/service name */
        if (host) {
                StringBuffer_append(C->sb, "//%s", host);
                if (port > 0)
                        StringBuffer_append(C->sb, ":%d", port);
                StringBuffer_append(C->sb, "/%s", servicename);
        } else /* Or just service name */
                StringBuffer_append(C->sb, "%s", servicename);
        // Set Connection ResultSet fetch size if found in URL
        const char *fetchSize = URL_getParameter(url, "fetch-size");
        
        if (fetchSize) {
                int rows = Str_parseInt(fetchSize);
                if (rows < 1)
                        ERROR("invalid fetch-size");
                Connection_setFetchSize(C->delegator, rows);
        }
        /* Create a server context */
        // C->lastError = DCIServerAttach(C->srv, C->err, StringBuffer_toString(C->sb), StringBuffer_length(C->sb), DCI_DEFAULT);
        // char str=StringBuffer_toString(C->sb);
        C->lastError = DCIServerAttach(C->srv, C->err, host, (sb4)strlen(host), DCI_DEFAULT);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
        {
                ORAERROR(C);
        }  
        /* Set attribute server context in the service context */
        C->lastError = DCIAttrSet(C->svc, DCI_HTYPE_SVCCTX, C->srv, 0, DCI_ATTR_SERVER, C->err);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
        {
                ORAERROR(C);
        }    
        C->lastError = DCIHandleAlloc(C->env, (void**)&C->usr, DCI_HTYPE_SESSION, 0, NULL);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
        {
                ORAERROR(C);
        }      
        C->lastError = DCIAttrSet(C->usr, DCI_HTYPE_SESSION, (dvoid *)username, (int)strlen(username), DCI_ATTR_USERNAME, C->err);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
        {
                ORAERROR(C);
        }    
        C->lastError = DCIAttrSet(C->usr, DCI_HTYPE_SESSION, (dvoid *)password, (int)strlen(password), DCI_ATTR_PASSWORD, C->err);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
        {
                ORAERROR(C);
        }      
        ub4 sessionFlags = DCI_DEFAULT;
        if (IS(URL_getParameter(url, "SYSDBA"), "true")) {
                sessionFlags |= DCI_SYSDBA;
        }
        // printf("%d\n",sessionFlags);
        C->lastError = DCISessionBegin(C->svc, C->err, C->usr, DCI_CRED_RDBMS, sessionFlags);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
        {
                ORAERROR(C);
        }
        DCIAttrSet(C->svc, DCI_HTYPE_SVCCTX, C->usr, 0, DCI_ATTR_SESSION, C->err);
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
                DCISessionEnd((*C)->svc, (*C)->err, (*C)->usr, DCI_DEFAULT);
                (*C)->svc = NULL;
        }
        if ((*C)->srv)
                DCIServerDetach((*C)->srv, (*C)->err, DCI_DEFAULT);
        if ((*C)->env)
                DCIHandleFree((*C)->env, DCI_HTYPE_ENV);
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
        C->lastError = DCIPing(C->svc, C->err, DCI_DEFAULT);
        return (C->lastError == DCI_SUCCESS);
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
                        DCISvcCtx* t = C->svc;
                        C->svc = NULL;
                        Thread_join(C->watchdog);
                        C->svc = t;
                        C->watchdog = 0;
                }
        }
}


static bool _beginTransaction(T C) {
        assert(C);
        if (C->txnhp == NULL) /* Allocate handler only once, if it is necessary */
        {
            /* allocate transaction handle and set it in the service handle */
            C->lastError = DCIHandleAlloc(C->env, (void **)&C->txnhp, DCI_HTYPE_TRANS, 0, 0);
            if (C->lastError != DCI_SUCCESS) 
                return false;
            DCIAttrSet(C->svc, DCI_HTYPE_SVCCTX, (void *)C->txnhp, 0, DCI_ATTR_TRANS, C->err);
        }
        C->lastError = DCITransStart (C->svc, C->err, ORACLE_TRANSACTION_PERIOD, DCI_TRANS_NEW);
        return (C->lastError == DCI_SUCCESS);
}


static bool _commit(T C) {
        assert(C);
        C->lastError = DCITransCommit(C->svc, C->err, DCI_DEFAULT);
        return C->lastError == DCI_SUCCESS;
}


static bool _rollback(T C) {
        assert(C);
        C->lastError = DCITransRollback(C->svc, C->err, DCI_DEFAULT);
        return C->lastError == DCI_SUCCESS;
}


static long long _lastRowId(T C) {
        /*:FIXME:*/
        /*
         Oracle's RowID can be mapped on string only
         so, currently I leave it unimplemented
         */
        
        /*     DCIRowid* rowid; */
        /*     DCIDescriptorAlloc((dvoid *)C->env,  */
        /*                        (dvoid **)&rowid, */
        /*                        (ub4) DCI_DTYPE_ROWID,  */
        /*                        (size_t) 0, (dvoid **) 0); */
        
        /*     if (DCIAttrGet (select_p, */
        /*                     DCI_HTYPE_STMT, */
        /*                     &rowid,              /\* get the current rowid *\/ */
        /*                     0, */
        /*                     DCI_ATTR_ROWID, */
        /*                     errhp)) */
        /*     { */
        /*         printf ("Getting the Rowid failed \n"); */
        /*         return (DCI_ERROR); */
        /*     } */
        
        /*     DCIDescriptorFree(rowid, DCI_DTYPE_ROWID); */
        DEBUG("OracleConnection_lastRowId: Not implemented yet");
        return -1;
}


static long long _rowsChanged(T C) {
        assert(C);
        return C->rowsChanged;
}


static bool _execute(T C, const char *sql, va_list ap) {
        DCIStmt* stmtp;
        va_list ap_copy;
        assert(C);
        C->rowsChanged = 0;
        va_copy(ap_copy, ap);
        StringBuffer_vset(C->sb, sql, ap_copy);
        va_end(ap_copy);
        StringBuffer_trim(C->sb);
        /* Build statement */
        C->lastError = DCIHandleAlloc(C->env, (void **)&stmtp, DCI_HTYPE_STMT, 0, NULL);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
                return false;
        C->lastError = DCIStmtPrepare(stmtp, C->err, StringBuffer_toString(C->sb), StringBuffer_length(C->sb), DCI_NTV_SYNTAX, DCI_DEFAULT);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO) {
                DCIHandleFree(stmtp, DCI_HTYPE_STMT);
                return false;
        }
        /* Execute */
        if (C->timeout > 0) {
                C->countdown = C->timeout;
                C->running = true;
        }
        C->lastError = DCIStmtExecute(C->svc, stmtp, C->err, 1, 0, NULL, NULL, DCI_DEFAULT);
        C->running = false;
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO) {
                ub4 parmcnt = 0;
                DCIAttrGet(stmtp, DCI_HTYPE_STMT, &parmcnt, NULL, DCI_ATTR_PARSE_ERROR_OFFSET, C->err);
                DEBUG("Error occured in StmtExecute %d (%s), offset is %d\n", C->lastError, _getLastError(C), parmcnt);
                DCIHandleFree(stmtp, DCI_HTYPE_STMT);
                return false;
        }
        C->lastError = DCIAttrGet(stmtp, DCI_HTYPE_STMT, &C->rowsChanged, 0, DCI_ATTR_ROW_COUNT, C->err);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
                DEBUG("OracleConnection_execute: Error in DCIAttrGet %d (%s)\n", C->lastError, _getLastError(C));
        DCIHandleFree(stmtp, DCI_HTYPE_STMT);
        return C->lastError == DCI_SUCCESS;
}


static ResultSet_T _executeQuery(T C, const char *sql, va_list ap) {
        
        DCIStmt* stmtp;
        va_list  ap_copy;
        assert(C);
        C->rowsChanged = 0;
        va_copy(ap_copy, ap);
        StringBuffer_vset(C->sb, sql, ap_copy);
        va_end(ap_copy);
        StringBuffer_trim(C->sb);
        /* Build statement */
        C->lastError = DCIHandleAlloc(C->env, (void **)&stmtp, DCI_HTYPE_STMT, 0, NULL);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
                return NULL;
        C->lastError = DCIStmtPrepare(stmtp, C->err, StringBuffer_toString(C->sb), StringBuffer_length(C->sb), DCI_NTV_SYNTAX, DCI_DEFAULT);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO) {
                DCIHandleFree(stmtp, DCI_HTYPE_STMT);
                return NULL;
        }
        /* Execute and create Result Set */
        if (C->timeout > 0) {
                C->countdown = C->timeout;
                C->running = true;
        }
        C->lastError = DCIStmtExecute(C->svc, stmtp, C->err, 0, 0, NULL, NULL, DCI_DEFAULT);
        C->running = false;
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO) {
                ub4 parmcnt = 0;
                DCIAttrGet(stmtp, DCI_HTYPE_STMT, &parmcnt, NULL, DCI_ATTR_PARSE_ERROR_OFFSET, C->err);
                DEBUG("Error occured in StmtExecute %d (%s), offset is %d\n", C->lastError, _getLastError(C), parmcnt);
                DCIHandleFree(stmtp, DCI_HTYPE_STMT);
                return NULL;
        }
        C->lastError = DCIAttrGet(stmtp, DCI_HTYPE_STMT, &C->rowsChanged, 0, DCI_ATTR_ROW_COUNT, C->err);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
                DEBUG("OracleConnection_execute: Error in DCIAttrGet %d (%s)\n", C->lastError, _getLastError(C));
        return ResultSet_new(OracleResultSet_new(C->delegator, stmtp, C->env, C->usr, C->err, C->svc, true), (Rop_T)&oraclerops);
}


static PreparedStatement_T _prepareStatement(T C, const char *sql, va_list ap) {
        DCIStmt *stmtp;
        va_list ap_copy;
        assert(C);
        va_copy(ap_copy, ap);
        StringBuffer_vset(C->sb, sql, ap_copy);
        va_end(ap_copy);
        StringBuffer_trim(C->sb);
        StringBuffer_prepare4oracle(C->sb);
        auto res=StringBuffer_toString(C->sb);
        /* Build statement */
        C->lastError = DCIHandleAlloc(C->env, (void **)&stmtp, DCI_HTYPE_STMT, 0, 0);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO)
                return NULL;
        C->lastError = DCIStmtPrepare(stmtp, C->err, StringBuffer_toString(C->sb), StringBuffer_length(C->sb), DCI_NTV_SYNTAX, DCI_DEFAULT);
        if (C->lastError != DCI_SUCCESS && C->lastError != DCI_SUCCESS_WITH_INFO) {
                DCIHandleFree(stmtp, DCI_HTYPE_STMT);
                return NULL;
        }
        return PreparedStatement_new(OraclePreparedStatement_new(C->delegator, stmtp, C->env, C->usr, C->err, C->svc), (Pop_T)&oraclepops);
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
