# Build and Run OpenGauss in a docker container

## Build Docker Container image for OpenGauss

We target for OpenGauss stable version V2.1.0. To build OpenGauss from source, we need both the openGauss-server and its dependencies in project openGauss-third_party. To avoid building the third party dependencies from source, we could download the [binary](https://opengauss.obs.cn-south-1.myhuaweicloud.com/2.1.0/openGauss-third_party_binarylibs.tar.gz) and use it directly.

Unfortunately, openGauss did not provide a docker file for V2.1.0, we created a file for V2.1.0 at docker/dockerfiles/dockerfile to help build the openGauss source code.

The build steps:
- Go to dockerfile fold:
```bash
cd docker/dockerfiles/
```
- Build docker image opengauss-server:
```bash
docker build -t opengauss-server  - < dockerfile
```
- During the docker image build process, we configure the openGauss server, for example, with this command, where GAUSSHOME is the location to install opengauss artifacts:
```bash
./configure --gcc-version=8.5.0 CC=g++ CFLAGS='-O0' --prefix=$GAUSSHOME --3rd=/tmp/openGauss-third_party_binarylibs
```
- Compile openGauss-server source code:
```bash
make -sj
```
- Install openGauss-server:
```bash
make install
```
The above command installs opengauss artifacts on the location that the environment variable GAUSSHOME is set to.
For example, if GAUSSHOME is set to /opt/opengauss with the following configuration,

```bash
# export GAUSSHOME=/opt/opengauss
# ./configure --gcc-version=8.5.0 CC=g++ CFLAGS='-O2 -g3' --prefix=$GAUSSHOME --3rd=/tmp/openGauss-third_party_binarylibs --enable-thread-safety --without-readline --without-zlib
```
To install the database, we need to copy the simpleInstall scripts in openGauss-server to the artifact directory and create a data directory and a logs directory to store data and log files, respectively. The directory layout is as follows.

```bash
$ tree -L 1 /opt/opengauss/
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

OpenGauss needs to use a dependent library that we could put into system lib directory /usr/lib64

```bash
$ ls /usr/lib64/huawei/
libsecurec.so
```

then configure the ldd path so that it could be found and loaded correctly.

```bash
$ cat /etc/ld.so.conf.d/huawei-x86_64.conf
/usr/lib64/huawei
```

OpenGauss cannot be run with the root user, as a result, we need to create a user, omm, as follows.

```bash
groupadd dbgrp
useradd omm
usermod -G dbgrp omm
```

We also set up a password, for example, "Test3456" for this account and change the artifact directory owner to this account.

```bash
chown omm.dbgrp -R /opt/opengauss
```
Finally, we need to set up environment variables for the user omm in .bashrc

```bash
export GS_CLUSTER_NAME=dbCluster
export GAUSSLOG=/opt/opengauss/logs
export PGDATA=/opt/opengauss/data
export GAUSSHOME=/opt/opengauss
export PATH=$GAUSSHOME/bin:$PATH
export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH
```
## Run OpenGauss in docker container

Once the above opengauss-server image was built from the dockerfile, we could configure opengauss and then run it.

First, run the container and logon with bash
```bash
docker run --privileged -it opengauss-server bash
```

Then change to user omm, configure and install the database

```bash
# su - omm
$ cd /opt/opengauss/simpleInstall/
$ sh install.sh -w Test3456
```
Type in "yes" when was asked for a demo database.

```
Would you like to create a demo database (yes/no)? yes
```

openGauss is running after the above script, then we could test with the demo database.

```bash
$ gsql -d finance
gsql ((GaussDB Kernel V500R002C00 build 590b0f8e) compiled at 2022-03-11 18:54:26 commit 0 last mr  )
Non-SSL connection (SSL connection is recommended when requiring high-security)
Type "help" for help.

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
```
