# Build and run chogori-opengauss using docker containers

Here, we demonstrate how to build and run chogori-opengauss. The instructions are organized into three sections. The first section demonstrates how to build the docker container images for the opengauss server and the [chogori-platform](https://github.com/futurewei-cloud/chogori-platform) (i.e., k2) runner. The second section demonstrates how to build and run chogori-opengauss using the images. The third section demonstrates how to build and run a vanilla openGauss server.
## 1. Build docker container images for chogori-opengauss

The chogori-opengauss project was forked from [openGauss-server v2.1.0](https://gitee.com/opengauss/openGauss-server/tree/v2.1.0). Unfortunately, openGauss did not provide a docker file for v2.1.0. We created a dockerfile for v2.1.0 at docker/dockerfiles/dockerfile to help build the opengauss source code.

We assume that the chogori-opengauss project has been downloaded (i.e., cloned), and CHOGORI_OPENGAUSS refers to the root directory of the project. For example, if the project is cloned into /home/demouser/workspace/, then CHOGORI_OPENGAUSS is /home/demouser/workspace/chogori-opengauss. If a relative path is used, we assume that the path prefix is CHOGORI_OPENGAUSS.

**Build the docker images:**

1. Go to the dockerfile folder (that is, go to the CHOGORI_OPENGAUSS/docker/dockerfiles folder):
```
$ cd docker/dockerfiles/
```
2. Build docker image named "opengauss-server":
```
$ docker build -t opengauss-server - < dockerfile
```
3. In the same folder, build chogori-platform (i.e., k2) runner image, named "k2runner":
```
$ docker build -t k2runner - < dockerfile_k2runner
```

The dockerfile for the opengauss-server image will take care of the following important things (in case you are curious). However, these steps should not be followed manually, as docker will make sure these steps have taken effect when the container image is built. You can jump to section 2 to build and run the opengauss server directly.

- The GAUSSHOME environment variable will be set as /opt/opengauss for the root user. This is where we will install all the gaussdb server artifacts.

- To build openGauss from source, we need both the source code and its dependencies in project [openGauss-third_party](https://github.com/opengauss-mirror/openGauss-third_party). To avoid building the third party dependencies from source, we download the [binary](https://opengauss.obs.cn-south-1.myhuaweicloud.com/2.1.0/openGauss-third_party_binarylibs.tar.gz) and use it directly.

- openGauss needs to use a dependent library (``` libsecurec.so```) that we could put into system lib directory /usr/lib64. The full path for the library will be /usr/lib64/huawei/libsecurec.so in the container image.

- openGauss cannot be run with the root user. As a result, we need to create a user, omm. We also set up a password, for example, "Test3456" for this user and change the artifact directory owner to this user.

- Finally, we need to set up environment variables for user omm in the user's .bashrc file.
## 2. Build and run opengauss server
We need to open two terminals on the host machine, one for the opengauss server and one for the k2 runner.

**Run the chogori-platform (i.e., k2) cluster**

In the terminal for k2 runner, go to the CHOGORI_OPENGAUSS directory. Use the following command to launch the container for k2 runner:
```
$ docker run -it --privileged -p 30000:30000 -v $PWD:/build:delegated --rm k2runner /build/simpleInstall/k2test/run_k2_cluster.sh
```
After this, the k2 cluster (within a container) will be live and running, waiting for the opengauss server to contact it. The k2 cluster will serve as the storage layer for opengauss server.

**Build and run chogori-opengauss**

In the terminal for opengauss server, go to the CHOGORI_OPENGAUSS directory. Use the following command to launch the container to build and run the opengauss server:
```
$ docker run -it --privileged -v $PWD:/build:delegated --rm -w /build opengauss-server bash
```
After this, we will be within the docker container as the root user in the /build directory. Note that our source code directory in the host machine is mounted as the /build directory in the container. Before building the opengauss server, we need to configure it, using the following command:
```
# ./configure --gcc-version=8.5.0 CC=g++ CFLAGS='-O2 -g3' --prefix=$GAUSSHOME --3rd=/openGauss-third_party_binarylibs --enable-mot --enable-thread-safety --without-readline --without-zlib
```
Build opengauss:
```
# make -j 8
```
The -j option allows specifying the degree of parallelism for make. Use a proper number according to your system's configuration (number of cores, amount of memory, etc.). Install opengauss:

```
# make -j 8 install
```
The above command will copy the opengauss artifacts to the location that the environment variable GAUSSHOME is set to. In our case, it is /opt/opengauss within the container.


To complete the opengauss installation, we need to copy the simpleInstall scripts in the source code to the installation directory and create a data directory and a logs directory to store data and log files, respectively. 
```
# cp -r /build/simpleInstall /opt/opengauss
# cd /opt/opengauss
# mkdir data
# mkdir logs
```
The directory layout is as follows.

```
# tree -L 1 /opt/opengauss/
/opt/opengauss/
├── bin
├── data
├── etc
├── include
├── jre
├── lib
├── logs
├── share
└── simpleInstall
```

Change the ownership of /opt/opengauss to the omm user.
```
# chown omm.dbgrp -R ${GAUSSHOME}
```

Then switch to the omm user, and run the opengauss database server using the ```pg_run.sh``` script:

```
# su - omm
$ cd /opt/opengauss/simpleInstall/
$ sh pg_run.sh -w Test3456
```
Type "no" when asked for a demo database.

```
Would you like to create a demo database (yes/no)? no
```
The original demo databases, ```finance``` and ```school``` (created by school.sql and finance.sql from the opengGauss-server project) are too large to fit in our k2 cluster running within a single container. After the pg_run.sh script finishes, we will have the database server (gaussdb) running, and we can use gsql to connect to the database server to create simpler databases.

Within the /opt/opengauss/simpleInstall directory, we have the finance_min.sql and school_min.sql files that create two databases, ```finance_min``` and ```school_min```, which are the simplified verions of the original ```finance``` and ```school``` databases, respectively. Within the /opt/opengauss/simpleInstall directory, using the omm user, execute the following command to connect to the "postgres" database:
```
$ gsql -d postgres
```
Then, within the gsql client terminal, we can create the simplified demo databases as follows:

```
openGauss=# \i finance_min.sql;
finance_min=# \i school_min.sql;
```

The first command will create the ```finance_min``` database, and connect to it. The second command will create the ```school_min``` database and connect to it. We can then read content from the tables.

```
school_min=# select * from course;
 cor_id |    cor_name    | cor_type | credit 
--------+----------------+----------+--------
      1 | 数据库系统概论 | 必修     |      3
      2 | 艺术设计概论   | 选修     |      1
      3 | 力学制图       | 必修     |      4
      4 | 飞行器设计历史 | 选修     |      1
(4 rows)
```

To connect to the ```finance_min``` database:
```
school_min=# \c finance_min;
```
Display the rows in the insurance table:
```
finance_min=# select * from insurance;
  i_name  | i_id | i_amount |      i_person      | i_year | i_project 
----------+------+----------+--------------------+--------+-----------
 健康保险 |    1 |     2000 | 老人               |     30 | 平安保险
 人寿保险 |    2 |     3000 | 老人               |     30 | 平安保险
 意外保险 |    3 |     5000 | 所有人             |     30 | 平安保险
 医疗保险 |    4 |     2000 | 所有人             |     30 | 平安保险
(4 rows)
```
Exit the gsql client terminal:
```
finance_min=# \q
```
Section 3 has more information on the gsql tool.

**TODO: run another gaussdb instance**

**TODO: run TPCC test against chogori-opengauss**

## 3. Build and run vanilla openGauss server 
The chogori-opengauss project was forked from [openGauss-server v2.1.0](https://gitee.com/opengauss/openGauss-server/tree/v2.1.0). We can build vanilla openGauss server v2.1.0 by folowing the instructions in this section. This can help us compare chogori-opengauss features against that of the vanilla openGauss-server. 

In a host terminal, go to the CHOGORI_OPENGAUSS folder.
Checkout openGauss server v2.1.0.
```
$ git checkout v2.1.0
```
Use the following command to launch the container to build and run the openGauss server:
```
$ docker run -it --privileged -v $PWD:/build:delegated --rm -w /build opengauss-server bash
```
Within the opengauss-server container, configure openGauss server:
```
# ./configure --gcc-version=8.5.0 CC=g++ CFLAGS='-O2 -g3' --prefix=$GAUSSHOME --3rd=/openGauss-third_party_binarylibs --enable-mot --enable-thread-safety --without-readline --without-zlib
```
Build and install openGauss:
```
# make -j 8
# make -j 8 install
```
To complete the opengauss installation, we need to copy the simpleInstall scripts in the source code to the installation directory and create a data directory and a logs directory to store data and log files, respectively. 
```
# cp -r /build/simpleInstall /opt/opengauss
# cd /opt/opengauss
# mkdir data
# mkdir logs
```
Change the ownership of /opt/opengauss to the omm user.
```
# chown omm.dbgrp -R ${GAUSSHOME}
```

Then, switch to the omm user and run the openGauss database server.

```
# su - omm
$ cd /opt/opengauss/simpleInstall/
$ sh install.sh -w Test3456
```


Some interesting log lines:

```
$ sh install.sh -w Test3456
[step 1]: check parameter
[step 2]: check install env and os setting
[step 3]: change_gausshome_owner
[step 4]: set environment variables

[step 6]: init datanode

creating directory /opt/opengauss/data/single_node ... ok
creating subdirectories ... ok
selecting default max_connections ... 100
selecting default shared_buffers ... 32MB
creating configuration files ... ok

initializing pg_authid ... ok
setting password ... ok
initializing dependencies ... ok
loading PL/pgSQL server-side language ... ok
creating system views ... ok
creating performance views ... ok
loading system objects' descriptions ... ok
creating collations ... ok
creating conversions ... ok
creating dictionaries ... ok
setting privileges on built-in objects ... ok
initialize global configure for bucketmap length ... ok
creating information schema ... ok
loading foreign-data wrapper for distfs access ... ok
loading foreign-data wrapper for hdfs access ... ok
loading foreign-data wrapper for log access ... ok
loading hstore extension ... ok
loading security plugin ... ok
update system tables ... ok
creating snapshots catalog ... ok
vacuuming database template1 ... ok
copying template1 to template0 ... ok
copying template1 to postgres ... ok
freezing database template0 ... ok
freezing database template1 ... ok
freezing database postgres ... ok

[step 7]: start datanode
[2022-03-11 19:59:43.681][252][][gs_ctl]: gs_ctl started,datadir is /opt/opengauss/data/single_node

[2022-03-11 19:59:45.796][252][][gs_ctl]:  done
[2022-03-11 19:59:45.796][252][][gs_ctl]: server started (/opt/opengauss/data/single_node)
import sql file
Would you like to create a demo database (yes/no)? yes
Load demoDB [school,finance] success.
[complete successfully]: You can start or stop the database server using:
    gs_ctl start|stop|restart -D $GAUSSHOME/data/single_node -Z single_node

```

openGauss is running after the above script. Then, we could test with the demo database.

**Test gsql for openGauss**

```sql
$ gsql -d finance
gsql ((GaussDB Kernel V500R002C00 build 590b0f8e) compiled at 2022-03-11 18:54:26 commit 0 last mr  )
Non-SSL connection (SSL connection is recommended when requiring high-security)
Type "help" for help.

finance=# \l
                              List of databases
   Name    | Owner | Encoding |   Collate   |    Ctype    | Access privileges
-----------+-------+----------+-------------+-------------+-------------------
 finance   | omm   | UTF8     | en_US.UTF-8 | en_US.UTF-8 |
 postgres  | omm   | UTF8     | en_US.UTF-8 | en_US.UTF-8 |
 school    | omm   | UTF8     | en_US.UTF-8 | en_US.UTF-8 |
 template0 | omm   | UTF8     | en_US.UTF-8 | en_US.UTF-8 | =c/omm           +
           |       |          |             |             | omm=CTc/omm
 template1 | omm   | UTF8     | en_US.UTF-8 | en_US.UTF-8 | =c/omm           +
           |       |          |             |             | omm=CTc/omm
(5 rows)

finance=# \d
                              List of relations
 Schema |       Name       | Type  | Owner |             Storage
--------+------------------+-------+-------+----------------------------------
 public | bank_card        | table | omm   | {orientation=row,compression=no}
 public | client           | table | omm   | {orientation=row,compression=no}
 public | finances_product | table | omm   | {orientation=row,compression=no}
 public | fund             | table | omm   | {orientation=row,compression=no}
 public | insurance        | table | omm   | {orientation=row,compression=no}
 public | property         | table | omm   | {orientation=row,compression=no}
(6 rows)

finance=# select * from fund;
   f_name    | f_id |      f_type       | f_amount |     risk_level      | f_manager
-------------+------+-------------------+----------+---------------------+-----------
 股票        |    1 | 股票型            |    10000 | 高                  |         1
 投资        |    2 | 债券型            |    10000 | 中                  |         2
 国债        |    3 | 货币型            |    10000 | 低                  |         3
 沪深300指数 |    4 | 指数型            |    10000 | 中                  |         4
(4 rows)

finance=# \q
```

To check all tables including system tables, run the following command in gsql.

```sql
finance=# \dS+
                                                      List of relations
   Schema   |               Name                | Type  | Owner |    Size    |             Storage              | Description
------------+-----------------------------------+-------+-------+------------+----------------------------------+-------------
 pg_catalog | get_global_prepared_xacts         | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_all_control_group_info         | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_asp                            | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_auditing                       | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_auditing_access                | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_auditing_policy                | table | omm   | 0 bytes    |                                  |
(232 rows)
```

To get help for gsql commands, run the following commands

```sql
finance=# \?
General
  \copyright             show openGauss usage and distribution terms
  \g [FILE] or ;         execute query (and send results to file or |pipe)
  \h(\help) [NAME]              help on syntax of SQL commands, * for all commands
  \parallel [on [num]|off] toggle status of execute (currently off)
  \q                     quit gsql

Query Buffer
  \e [FILE] [LINE]       edit the query buffer (or file) with external editor
  \ef [FUNCNAME [LINE]]  edit function definition with external editor
  \p                     show the contents of the query buffer
  \r                     reset (clear) the query buffer
  \w FILE                write query buffer to file

Input/Output
  \copy ...              perform SQL COPY with data stream to the client host
  \echo [STRING]         write string to standard output
  \i FILE                execute commands from file
  \i+ FILE KEY           execute commands from encrypted file
  \ir FILE               as \i, but relative to location of current script
  \ir+ FILE KEY          as \i+, but relative to location of current script
  \o [FILE]              send all query results to file or |pipe
  \qecho [STRING]        write string to query output stream (see \o)

Informational
  (options: S = show system objects, + = additional detail)
  \d[S+]                 list tables, views, and sequences
  \d[S+]  NAME           describe table, view, sequence, or index
  \da[S]  [PATTERN]      list aggregates
  \db[+]  [PATTERN]      list tablespaces
  \dc[S+] [PATTERN]      list conversions
  \dC[+]  [PATTERN]      list casts
  \dd[S]  [PATTERN]      show object descriptions not displayed elsewhere
  \ddp    [PATTERN]      list default privileges
  \dD[S+] [PATTERN]      list domains
  \ded[+] [PATTERN]      list data sources
  \det[+] [PATTERN]      list foreign tables
  \des[+] [PATTERN]      list foreign servers
  \deu[+] [PATTERN]      list user mappings
  \dew[+] [PATTERN]      list foreign-data wrappers
  \df[antw][S+] [PATRN]  list [only agg/normal/trigger/window] functions
  \dF[+]  [PATTERN]      list text search configurations
  \dFd[+] [PATTERN]      list text search dictionaries
  \dFp[+] [PATTERN]      list text search parsers
  \dFt[+] [PATTERN]      list text search templates
  \dg[+]  [PATTERN]      list roles
  \di[S+] [PATTERN]      list indexes
  \dl                    list large objects, same as \lo_list
  \dL[S+] [PATTERN]      list procedural languages
  \dn[S+] [PATTERN]      list schemas
  \do[S]  [PATTERN]      list operators
  \dO[S+] [PATTERN]      list collations
  \dp     [PATTERN]      list table, view, and sequence access privileges
  \drds [PATRN1 [PATRN2]] list per-database role settings
  \ds[S+] [PATTERN]      list sequences
  \dt[S+] [PATTERN]      list tables
  \dT[S+] [PATTERN]      list data types
  \du[+]  [PATTERN]      list roles
  \dv[S+] [PATTERN]      list views
  \dE[S+] [PATTERN]      list foreign tables
  \dx[+]  [PATTERN]      list extensions
  \l[+]                  list all databases
  \sf[+] FUNCNAME        show a function's definition
  \z      [PATTERN]      same as \dp

Formatting
  \a                     toggle between unaligned and aligned output mode
  \C [STRING]            set table title, or unset if none
  \f [STRING]            show or set field separator for unaligned query output
  \H                     toggle HTML output mode (currently off)
  \pset NAME [VALUE]     set table output option
                         (NAME := {format|border|expanded|fieldsep|fieldsep_zero|footer|null|
                         numericlocale|recordsep|recordsep_zero|tuples_only|title|tableattr|pager})
  \t [on|off]            show only rows (currently off)
  \T [STRING]            set HTML <table> tag attributes, or unset if none
  \x [on|off|auto]       toggle expanded output (currently off)

Connection
  \c[onnect] [DBNAME|- USER|- HOST|- PORT|-]
                         connect to new database (currently "finance")
  \encoding [ENCODING]   show or set client encoding
  \conninfo              display information about current connection

Operating System
  \cd [DIR]              change the current working directory
  \setenv NAME [VALUE]   set or unset environment variable
  \timing [on|off]       toggle timing of commands (currently off)
  \! [COMMAND]           execute command in shell or start interactive shell

Variables
  \prompt [TEXT] NAME    prompt user to set internal variable
  \set [NAME [VALUE]]    set internal variable, or list all if no parameters
  \unset NAME            unset (delete) internal variable

Large Objects
  \lo_export LOBOID FILE
  \lo_import FILE [COMMENT]
  \lo_list
  \lo_unlink LOBOID      large object operations
```
