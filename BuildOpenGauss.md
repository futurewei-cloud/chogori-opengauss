# Build and run chogori-opengauss using docker containers

Here, we demonstrate how to build and run chogori-opengauss. The instructions are organized into three sections. Section 1 demonstrates how to build the docker container images for the opengauss server and the [chogori-platform](https://github.com/futurewei-cloud/chogori-platform) (i.e., k2) runner. Section 2 demonstrates how to build and run chogori-opengauss using the images. Specifically, Section 2.3 shows how to run multiple database server instances against the same k2 cluster, and Section 2.4 demonstrates how to run TPCC test against chogori-opengauss database server. Section 3 demonstrates how to build and run a vanilla openGauss server.

Some conventions we are following:

- The "```$```" prompt means that the command is executed as a non-root user.
- The "```#```" prompt means that the command is executed as the root user (usually within a docker container).
- The "```DBNAME=#```" prompt means that the command is executed within the ```gsql``` terminal.

## 1. Build docker container images for chogori-opengauss

The chogori-opengauss project was forked from [openGauss-server v2.1.0](https://gitee.com/opengauss/openGauss-server/tree/v2.1.0). Unfortunately, openGauss did not provide a docker file for v2.1.0. We created a dockerfile for v2.1.0 at ```docker/dockerfiles/dockerfile``` to help build the opengauss source code.

We assume that the chogori-opengauss project has been downloaded (i.e., cloned), and ```CHOGORI_OPENGAUSS``` refers to the root directory of the project. For example, if the project is cloned into ```/home/demouser/workspace```, then ```CHOGORI_OPENGAUSS``` is ```/home/demouser/workspace/chogori-opengauss```.

**Build the docker images:**

1. Go to the dockerfile folder (that is, go to the ```CHOGORI_OPENGAUSS/docker/dockerfiles``` folder):
```
$ cd chogori-opengauss/docker/dockerfiles/
```
2. Build the docker image named "```opengauss-server```":
```
$ docker build -t opengauss-server - < dockerfile
```
3. In the same folder, build the docker image named "```k2runner```", for chogori-platform (i.e., k2) cluster:
```
$ docker build -t k2runner - < dockerfile_k2runner
```

The dockerfile for the ```opengauss-server``` image will take care of the following important things (in case you are curious). However, these steps should not be followed manually, as docker will make sure these steps have taken effect when the container image is built. You can jump to Section 2 to build and run the opengauss server directly.

- The ```GAUSSHOME``` environment variable will be set as ```/opt/opengauss``` for the root user. This is where we will install all the gaussdb server artifacts.

- To build openGauss from source, we need both the source code and its dependencies in project [openGauss-third_party](https://github.com/opengauss-mirror/openGauss-third_party). To avoid building the third party dependencies from source, we download the [binary](https://opengauss.obs.cn-south-1.myhuaweicloud.com/2.1.0/openGauss-third_party_binarylibs.tar.gz) and use it directly.

- openGauss needs to use a dependent library (```libsecurec.so```) that we could put into system lib directory /usr/lib64. The full path for the library will be /usr/lib64/huawei/libsecurec.so in the container image.

- openGauss cannot be run with the root user. As a result, we need to create a user, ```omm```. We also set up a password, for example, ```Test3456``` for this user and change the artifact directory owner to this user.

- Finally, we need to set up environment variables for user ```omm``` in the user's ```.bashrc``` file.
## 2. Build and run chogori-opengauss
We need to open two command terminals on the host machine, one for the chogori-platform (i.e., k2) cluster and one for the opengauss server.

### 2.1 Run the chogori-platform (i.e., k2) cluster
Our script to run k2 cluster requires Asynchronous non-blocking IO (AIO). We need to increase the maximum number of concurrent requests for AIO. Run this on the host machine:
```
$ sudo bash -c "echo 1048576 > /proc/sys/fs/aio-max-nr"
```
Also, our k2 cluster uses huge pages. Run the following commands to set up huge pages:
```
$ sudo hugeadm --pool-pages-min 2MB:16000
$ sudo hugeadm --pool-pages-max 2MB:20000
```

In the terminal for k2 cluster, go to the ```CHOGORI_OPENGAUSS``` directory. Use the following command to launch the container for k2 cluster:
```
$ docker run -it --privileged -p 30000:30000 -v $PWD:/build:delegated --rm k2runner /build/simpleInstall/k2test/run_k2_cluster.sh
```
Some explanations on the options above: 
- "```-p 30000:30000```" exposes the container's port 30000 to the host machine, so that it is accessible by chogori-openguass server.
- "```-v $PWD:/build:delegated```" mounts the current working directory, i.e., ```CHOGORI_OPENGAUSS``` as the ```/build``` directory within the container. 
- "```k2runner```" is the docker image built in the previous section.
- "```/build/simpleInstall/k2test/run_k2_cluster.sh```" is the script we will run within the container to start the k2 cluster. 

After this, the k2 cluster (within a container) will be live and running, waiting for the opengauss server to contact it. The k2 cluster will serve as the storage for opengauss server.

### 2.2 Build and run opengauss server:

In the terminal for opengauss server, go to the ```CHOGORI_OPENGAUSS``` directory. Use the following command to launch the container to build and run the opengauss server:
```
$ docker run -it --privileged --net=host -v $PWD:/build:delegated --rm -w /build opengauss-server bash
```
Here, we use the host networking mode for the ```opengauss-server``` container. This will make running TPCC test simpler, as we will also use a host mode container for the TPCC test client.  If we do not use the host mode, the ```opengauss-server``` container and the TPCC test client container will use the (default) bridge networking mode. Then, the connection from the client container to the server container will be remote. Allowing remote connections to the gaussdb server will require changes in gaussdb's settings files (for example, postgres.conf and pg_hba.conf), and we want to avoid that. For this reason, we are using the host networking mode.

After this, we will be within the docker container as the root user in the ```/build``` directory. Note that our source code directory in the host machine is mounted as the ```/build``` directory in the container. Before building the opengauss server, we need to configure it, using the following command:
```
# ./configure --gcc-version=8.5.0 CC=g++ CFLAGS='-O2 -g3' --prefix=$GAUSSHOME --3rd=/openGauss-third_party_binarylibs --enable-mot --enable-thread-safety --without-readline --without-zlib
```
Build opengauss:
```
# make -j 8
```
The ```-j``` option allows specifying the degree of parallelism for make. Use a proper number according to your machine's configuration (number of cores, amount of memory, etc.). If the build fails, you can use a smaller number such as 4 or 2, or just do ```make``` without ```-j```. Install opengauss:

```
# make -j 8 install
```
The above command will copy the opengauss artifacts to the location that the environment variable ```GAUSSHOME``` is set to. In our case, it is ```/opt/opengauss``` within the container. To complete the opengauss installation, we need to copy the simpleInstall scripts in the source code to the installation directory and create a data directory and a logs directory to store data and log files, respectively:
```
# cp -r /build/simpleInstall /opt/opengauss
# cd /opt/opengauss
# mkdir data
# mkdir logs
```
The directory layout is as follows:

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

Change the ownership of ```/opt/opengauss``` to the ```omm``` user:
```
# chown omm.dbgrp -R ${GAUSSHOME}
```

Then, switch to the ```omm``` user, and run the opengauss database server using the ```pg_run.sh``` script:

```
# su omm
$ cd /opt/opengauss/simpleInstall/
$ sh pg_run.sh -w Test3456
```
Type "no" when asked for creating a demo database:

```
Would you like to create a demo database (yes/no)? no
```
The original demo databases, ```finance``` and ```school``` (created by ```school.sql``` and ```finance.sql``` from the opengGauss-server project) are too large to fit in our k2 cluster running within a single container. Besides, we do not support some features in the vanilla openGauss server. After the ```pg_run.sh``` script finishes, we will have the database server (```gaussdb```) running, and we can use ```gsql``` to connect to the database server to create simpler databases.

We added ```finance_min.sql``` and ```school_min.sql``` that create simplified tables of the ```finance``` and ```school``` databases. We create these simplified tables within the ```testdb``` database. Within the ```/opt/opengauss/simpleInstall``` directory, using the ```omm``` user, execute the following command to connect to the ```postgres``` database:
```
$ gsql -d postgres
```
Then, within the ```gsql``` terminal, create the ```testdb``` database.

```sql
openGauss=# CREATE DATABASE testdb;
```
Make sure to end the command with the semicolon. Otherwise, the command will not be executed. Then, connect to the ```testdb``` database:
```sql
openGauss=# \c testdb
```
We can then create the simplified tables: 
```sql
testdb=# \i finance_min.sql
testdb=# \i school_min.sql
```
The above commands will find the ```finance_min.sql``` and ```school_min.sql``` files and execute the SQL statements within them.  Check content of the course table:
```sql
testdb=# SELECT * FROM course;
 cor_id |    cor_name    | cor_type | credit 
--------+----------------+----------+--------
      1 | 数据库系统概论 | 必修     |      3
      2 | 艺术设计概论   | 选修     |      1
      3 | 力学制图       | 必修     |      4
      4 | 飞行器设计历史 | 选修     |      1
(4 rows)
```
Check content of the insurance table:
```sql
testdb=# SELECT * FROM insurance;
  i_name  | i_id | i_amount |      i_person      | i_year | i_project 
----------+------+----------+--------------------+--------+-----------
 健康保险 |    1 |     2000 | 老人               |     30 | 平安保险
 人寿保险 |    2 |     3000 | 老人               |     30 | 平安保险
 意外保险 |    3 |     5000 | 所有人             |     30 | 平安保险
 医疗保险 |    4 |     2000 | 所有人             |     30 | 平安保险
(4 rows)
```
Exit the ```gsql``` client terminal:
```sql
testdb=# \q
```
(Section 3 has more information on the ```gsql``` tool.)


### 2.3 Run multiple gaussdb instances using the same running k2 cluster:
After we have gone through the steps above, we have the k2 cluster still running. And the gaussdb process is also running in the background. Notice that they are running in two different containers. In chogori-opengauss, we can run multiple gaussdb instances using the same k2 cluster as the underlying storage. We can use the same ```opengauss-server``` container to simulate running another gaussdb instance using the same running k2 cluster. To do this, we could just stop the gaussdb process, remove everything inside the ```/opt/opengauss/data``` directory within the container, and then run the ```/opt/opengauss/simpleInstall/local_run.sh``` script. 

Here, we will demonstrate how to really run multiple gaussdb instances using the same running k2 cluster.

Keep the k2 cluster running in the k2runner container. Open another command terminal in the host machine. Go to the ```CHOGORI_OPENGAUSS``` directory. Use the following command to launch another container for the second gaussdb instance:
```
$ docker run -it --privileged --net=host -v $PWD:/build:delegated --rm -w /build opengauss-server bash
```
After entering the container, configure, build, and install chogori-opengauss using the same steps as that in the previous subsection:
```
# ./configure --gcc-version=8.5.0 CC=g++ CFLAGS='-O2 -g3' --prefix=$GAUSSHOME --3rd=/openGauss-third_party_binarylibs --enable-mot --enable-thread-safety --without-readline --without-zlib
# make -j 8
# make -j 8 install
# cp -r /build/simpleInstall /opt/opengauss
# cd /opt/opengauss
# mkdir data
# mkdir logs
# chown omm.dbgrp -R /opt/opengauss
```
Then, switch to the ```omm``` user and launch the second gaussdb instance, now using the ```local_run.sh``` script:
```
# su omm
$ cd /opt/opengauss/simpleInstall
$ sh local_run.sh -w Test3456 -p 5442
```
Here, we specify a different port number ```5442```, because the default port number ```5432``` may still be used by the first gaussdb instance, and we don't want to have any conflicts. Type "no" when asked for creating a demo database. After this, we will have the second gaussdb instance running in the second ```opengauss-server``` container.

Within either container for opengauss server, we can use ```gsql``` to connect to the gaussdb server. We are able to see the same information through these two gaussdb servers, as they have the same k2 cluster as the storeage, which is still running in the ```k2runner``` container. 

For example, in the second ```opengauss-server``` container, we can connect to the second gaussdb instance:
```
$ gsql -d postgres -p 5442
```
Make sure to specify the same port number we used to launch the second gaussdb instance. Then, connect to the ```testdb``` database and read some tables.
```sql
openGauss=# \c testdb
testdb=# SELECT * FROM insurance;
  i_name  | i_id | i_amount |      i_person      | i_year | i_project 
----------+------+----------+--------------------+--------+-----------
 健康保险 |    1 |     2000 | 老人               |     30 | 平安保险
 人寿保险 |    2 |     3000 | 老人               |     30 | 平安保险
 意外保险 |    3 |     5000 | 所有人             |     30 | 平安保险
 医疗保险 |    4 |     2000 | 所有人             |     30 | 平安保险
(4 rows)
```
The content of the insurance table should be the same as what we saw before.

### 2.4 Run TPCC test with chogori-opengauss
We use our [chogori-oltpbench](https://github.com/futurewei-cloud/chogori-oltpbench) project to carry out TPCC test for chogori-opengauss. We have made some tweaks based on the original [oltpbench](https://github.com/oltpbenchmark/oltpbench) project to make it work with chogori-opengauss. The tweaks are within the ```chogori-opengauss``` branch of the chogori-oltpbench project. 

We will be running the TPCC test client within a container, and we can use a public image on docker hub:  ```openjdk:16-slim-buster```. First, pull this image to our local host:
```
$ docker pull openjdk:16-slim-buster
```

Clone the [chogori-oltpbench](https://github.com/futurewei-cloud/chogori-oltpbench) project into your workspace, say ```/home/demouser/workspace```. The root directory of the oltpbench project will be ```/home/demouser/workspace/chogori-oltpbench```. Go to the project directory:
```
$ cd /home/demouser/workspace/chogori-oltpbench
```
Of course, you should use the correct directory based on your case. Checkout and switch to the ```chogori-opengauss``` branch:
```
$ git checkout chogori-opengauss
```
Launch the TPCC test container:
```
$ docker run -it --privileged --net=host -v $PWD:/host_mount:delegated --rm -w /host_mount openjdk:16-slim-buster bash
```
We use the host networking mode as mentioned before. The project directory in the host machine, ```/home/demouser/workspace/chogori-oltpbench```is mounted as ```/host_mount``` in the container. And we are in the ```/host_mount``` directory within the container. Install dependencies and build the chogori-oltpbench project:
```
# ./.deploy/install.sh
```
Note that if you want to redo the above step, make sure to clone the chogori-oltpbench project again, because the above step will move (instead of copy) a file in the project to a different location. If you don't clone the chogori-oltpbench again, the file will not be found if you redo the above step.

We will use the ```testdb``` database to conduct the TPCC test. Our ```run_k2_cluster.sh``` script has made sure we have enough memory for the test. We've configured the oltpbench test client to use the ```testdb``` database (see ```chogori-oltpbench/config/tpcc_config_postgres.xml```). If you want to use another database to do the test, you need to change several things:
- Add the database name to the collection list in ```chogori-opengauss/simpleInstall/k2config_pgrun.json```. Otherwise, we cannot create the new database.
- Add more endpoints to the nodepool endpoints in ```chogori-opengauss/simpleInstall/k2test/run_k2_cluster.sh```. Each new database needs at least one more core. Also, allocate more memory for the nodepool to make sure each core has least 1G memory.
- Change the database name in ```chogori-oltpbench/config/tpcc_config_postgres.xml``` from ```testdb``` to the name of your new database. 

After these changes, you need to reinstall chogori-opengauss, rerun the k2 cluster, and rerun chogori-opengauss. Again, we will just use the ```testdb``` database for TPCC test. So, we don't need to change any files. 

Switch to the terminal for opengauss server. Within the container, go to the ```/opt/opengauss/simpleInstall``` folder. Connect to the ```testdb``` database (if not already connected):
```
$ gsql -d testdb
```
Create the tables needed for TPCC test:
```sql
testdb=# \i k2test/tpcc_ddl.sql
```
The above command will find the ```/opt/opengauss/simpleInstall/k2test/tpcc_ddl.sql``` file and execute the SQL statements within it. After this, we can load TPCC test: 
```
# ./oltpbenchmark -b tpcc -c config/tpcc_config_postgres.xml --create=false --load=true --execute=false
```
You should see similar output as follows:
```
21:45:23,153 (DBWorkload.java:270) INFO  - ======================================================================

Benchmark:     TPCC {com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark}
Configuration: config/tpcc_config_postgres.xml
Type:          POSTGRES
Driver:        org.postgresql.Driver
URL:           jdbc:postgresql://localhost:5432/tpcc
Isolation:     TRANSACTION_READ_COMMITTED
Scale Factor:  1.0

21:45:23,154 (DBWorkload.java:271) INFO  - ======================================================================
21:45:23,174 (DBWorkload.java:559) INFO  - Loading data into TPCC database with 1 threads...
22:01:20,050 (DBWorkload.java:563) INFO  - Finished!
22:01:20,051 (DBWorkload.java:564) INFO  - ======================================================================
22:01:20,052 (DBWorkload.java:598) INFO  - Skipping benchmark workload execution
```

Execute TPCC test:
```
# ./oltpbenchmark -b tpcc -c config/tpcc_config_postgres.xml --create=false --load=false --execute=true -o outputfile
```
You should see similar output as follows:
```
22:04:30,221 (DBWorkload.java:270) INFO  - ======================================================================

Benchmark:     TPCC {com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark}
Configuration: config/tpcc_config_postgres.xml
Type:          POSTGRES
Driver:        org.postgresql.Driver
URL:           jdbc:postgresql://localhost:5432/tpcc
Isolation:     TRANSACTION_READ_COMMITTED
Scale Factor:  1.0

22:04:30,223 (DBWorkload.java:271) INFO  - ======================================================================
22:04:30,240 (DBWorkload.java:849) INFO  - Creating 1 virtual terminals...
22:04:33,018 (DBWorkload.java:854) INFO  - Launching the TPCC Benchmark with 1 Phase...
22:04:33,028 (ThreadBench.java:341) INFO  - PHASE START :: [Workload=TPCC] [Serial=false] [Time=200] [WarmupTime=0] [Rate=10000] [Arrival=REGULAR] [Ratios=[100.0, 0.0, 0.0, 0.0, 0.0]] [ActiveWorkers=1]
22:04:33,029 (ThreadBench.java:492) INFO  - MEASURE :: Warmup complete, starting measurements.
22:07:53,032 (ThreadBench.java:447) INFO  - TERMINATE :: Waiting for all terminals to finish ..
22:26:30,806 (ThreadBench.java:508) INFO  - Attempting to stop worker threads and collect measurements
22:26:30,807 (ThreadBench.java:247) INFO  - Starting WatchDogThread
22:26:30,808 (DBWorkload.java:860) INFO  - ======================================================================
22:26:30,813 (DBWorkload.java:861) INFO  - Rate limited reqs/s: Results(nanoSeconds=200000742364, measuredRequests=1) = 0.004999981440968888 requests/sec
22:26:30,818 (DBWorkload.java:700) INFO  - Output Raw data into file: results/outputfile.2.csv
```

## 3. Build and run vanilla openGauss server 
The chogori-opengauss project was forked from [openGauss-server v2.1.0](https://gitee.com/opengauss/openGauss-server/tree/v2.1.0). We can build and run vanilla openGauss server v2.1.0 by folowing the instructions in this section. This can help us compare chogori-opengauss features against that of the vanilla openGauss-server. 

In a host terminal, go to the ```CHOGORI_OPENGAUSS``` folder.
Checkout openGauss server v2.1.0:
```
$ git checkout v2.1.0
```
Use the following command to launch the container to build and run the openGauss server:
```
$ docker run -it --privileged --net=host -v $PWD:/build:delegated --rm -w /build opengauss-server bash
```
Within the ```opengauss-server``` container, configure, build and install openGauss server:
```
# ./configure --gcc-version=8.5.0 CC=g++ CFLAGS='-O2 -g3' --prefix=$GAUSSHOME --3rd=/openGauss-third_party_binarylibs --enable-mot --enable-thread-safety --without-readline --without-zlib
# make -j 8
# make -j 8 install
```
To complete the opengauss installation, we need to copy the simpleInstall scripts in the source code to the installation directory and create a data directory and a logs directory to store data and log files, respectively:
```
# cp -r /build/simpleInstall /opt/opengauss
# cd /opt/opengauss
# mkdir data
# mkdir logs
```
Change the ownership of /opt/opengauss to the ```omm``` user.
```
# chown omm.dbgrp -R ${GAUSSHOME}
```

Then, switch to the ```omm``` user and run the openGauss database server, using the ```install.sh``` script:

```
# su omm
$ cd /opt/opengauss/simpleInstall/
$ sh install.sh -w Test3456
```


Type "yes" when asked for creating a demo database. Some interesting log lines:

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
