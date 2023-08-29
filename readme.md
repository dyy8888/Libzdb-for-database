## 先决条件

```
需要事先下载达梦数据库的OCI驱动，可以在光盘的driver文件夹下看到dci的文件夹，将其下载到本地，并记录其路径
适配使用的是oracle类似的驱动，所以底层修改了oracle相关的代码
```

## 安装部署

### 1.下载源代码

```shell
git clone https://github.com/dyy8888/LibzdbForDM.git
```

### 2.修改

```shell
//修改makefile的第351行，路径下包含DCI.h和DCI1.h
CPPFLAGS = -I/home/dyy/dci/include
//修改makefile的第357行，路径下包含DCI.h和DCI1.h
DBCPPFLAGS =  -I/home/dyy/dci/include
//修改makefile中其他有关的路径，需检查所有相关路径，改成自己本地的路径
ACLOCAL = ${SHELL} /home/dyy/libzdb-3.2.3/config/missing aclocal-1.16
AMTAR = $${TAR-tar}
AM_DEFAULT_VERBOSITY = 1
AR = ar
AUTOCONF = ${SHELL} /home/dyy/libzdb-3.2.3/config/missing autoconf
AUTOHEADER = ${SHELL} /home/dyy/libzdb-3.2.3/config/missing autoheader
AUTOMAKE = ${SHELL} /home/dyy/libzdb-3.2.3/config/missing automake-1.16
//检查第358和378行使用dmdci的驱动
//如果电脑本身包含mysql的驱动，那么这个位置可能会包含-lmysqlclient，删除并替换
DBLDFLAGS =  -L/usr/lib/x86_64-linux-gnu -ldmdci -lpthread -ldl -lssl -lcrypto -lresolv -lm -lrt
LDFLAGS = -L/usr/lib/x86_64-linux-gnu -ldmdci -lpthread -ldl -lssl -lcrypto -lresolv -lm -lrt 
//检查第180-194行，注释掉其他数据库相关的加入，仅保留oracle
am__objects_1 = src/net/URL.lo
# am__objects_2 = src/db/mysql/MysqlConnection.lo \
# 	src/db/mysql/MysqlResultSet.lo \
# 	src/db/mysql/MysqlPreparedStatement.lo
#am__objects_3 = src/db/postgresql/PostgresqlConnection.lo \
#	src/db/postgresql/PostgresqlResultSet.lo \
#	src/db/postgresql/PostgresqlPreparedStatement.lo
#am__objects_4 = src/db/sqlite/SQLiteConnection.lo \
#	src/db/sqlite/SQLiteResultSet.lo \
#	src/db/sqlite/SQLitePreparedStatement.lo \
#	src/db/sqlite/SQLiteAdapter.lo
am__objects_5 = src/db/oracle/OracleConnection.lo \
	src/db/oracle/OracleResultSet.lo \
	src/db/oracle/OraclePreparedStatement.lo \
	src/db/oracle/OracleAdapter.lo
//96行-114行类似
# am__append_2 = src/db/mysql/MysqlConnection.c \
#                      src/db/mysql/MysqlResultSet.c \
#                      src/db/mysql/MysqlPreparedStatement.c

#am__append_3 = src/db/postgresql/PostgresqlConnection.c \
#                     src/db/postgresql/PostgresqlResultSet.c \
#                     src/db/postgresql/PostgresqlPreparedStatement.c

#am__append_4 = src/db/sqlite/SQLiteConnection.c \
#                     src/db/sqlite/SQLiteResultSet.c \
#                     src/db/sqlite/SQLitePreparedStatement.c \
#                     src/db/sqlite/SQLiteAdapter.c

am__append_5 = src/db/oracle/OracleConnection.c \
                    src/db/oracle/OracleResultSet.c \
                    src/db/oracle/OraclePreparedStatement.c \
                    src/db/oracle/OracleAdapter.c
//修改src目录下xconfig.h文件第41行左右，关闭其他数据库（直接注释掉）
#define HAVE_ORACLE 1
```

### 3.执行

```shell
make
sudo make install
```

生成文件将会出现在工程的`.libs`文件夹中