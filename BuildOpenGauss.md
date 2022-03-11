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
 pg_catalog | gs_auditing_policy_access         | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_auditing_policy_filters        | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_auditing_policy_privileges     | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_auditing_privilege             | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_client_global_keys             | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_client_global_keys_args        | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_cluster_resource_info          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_column_keys                    | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_column_keys_args               | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_comm_proxy_thread_status       | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_encrypted_columns              | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_encrypted_proc                 | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_file_stat                      | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_get_control_group_info         | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_global_chain                   | table | omm   | 8192 bytes |                                  |
 pg_catalog | gs_global_config                  | table | omm   | 40 kB      |                                  |
 pg_catalog | gs_instance_time                  | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_labels                         | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_masking                        | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_masking_policy                 | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_masking_policy_actions         | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_masking_policy_filters         | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_matview                        | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_matview_dependency             | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_matviews                       | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_model_warehouse                | table | omm   | 8192 bytes |                                  |
 pg_catalog | gs_obsscaninfo                    | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_opt_model                      | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_os_run_info                    | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_package                        | table | omm   | 8192 bytes |                                  |
 pg_catalog | gs_policy_label                   | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_recyclebin                     | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_redo_stat                      | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_session_cpu_statistics         | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_session_memory                 | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_session_memory_context         | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_session_memory_detail          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_session_memory_statistics      | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_session_stat                   | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_session_time                   | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_shared_memory_detail           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_sql_count                      | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_stat_session_cu                | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_thread_memory_context          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_total_memory_detail            | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_total_nodegroup_memory_detail  | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_txn_snapshot                   | table | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_cgroup_info                | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_ec_operator_history        | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_ec_operator_info           | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 pg_catalog | gs_wlm_ec_operator_statistics     | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_instance_history           | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 pg_catalog | gs_wlm_operator_history           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_operator_info              | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 pg_catalog | gs_wlm_operator_statistics        | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_plan_encoding_table        | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 pg_catalog | gs_wlm_plan_operator_history      | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_plan_operator_info         | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 pg_catalog | gs_wlm_rebuild_user_resource_pool | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_resource_pool              | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_session_history            | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_session_info               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_session_info_all           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_session_query_info_all     | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 pg_catalog | gs_wlm_session_statistics         | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_user_info                  | view  | omm   | 0 bytes    |                                  |
 pg_catalog | gs_wlm_user_resource_history      | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 pg_catalog | gs_wlm_workload_records           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | mpp_tables                        | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_aggregate                      | table | omm   | 48 kB      |                                  |
 pg_catalog | pg_am                             | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_amop                           | table | omm   | 104 kB     |                                  |
 pg_catalog | pg_amproc                         | table | omm   | 64 kB      |                                  |
 pg_catalog | pg_app_workloadgroup_mapping      | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_attrdef                        | table | omm   | 48 kB      |                                  |
 pg_catalog | pg_attribute                      | table | omm   | 1448 kB    |                                  |
 pg_catalog | pg_auth_history                   | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_auth_members                   | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_authid                         | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_available_extension_versions   | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_available_extensions           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_cast                           | table | omm   | 56 kB      |                                  |
 pg_catalog | pg_class                          | table | omm   | 408 kB     |                                  |
 pg_catalog | pg_collation                      | table | omm   | 48 kB      |                                  |
 pg_catalog | pg_comm_delay                     | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_comm_recv_stream               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_comm_send_stream               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_comm_status                    | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_constraint                     | table | omm   | 48 kB      |                                  |
 pg_catalog | pg_control_group_config           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_conversion                     | table | omm   | 56 kB      |                                  |
 pg_catalog | pg_cursors                        | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_database                       | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_db_role_setting                | table | omm   | 8192 bytes |                                  |
 pg_catalog | pg_default_acl                    | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_depend                         | table | omm   | 496 kB     |                                  |
 pg_catalog | pg_description                    | table | omm   | 184 kB     |                                  |
 pg_catalog | pg_directory                      | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_enum                           | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_ext_stats                      | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_extension                      | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_extension_data_source          | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_foreign_data_wrapper           | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_foreign_server                 | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_foreign_table                  | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_get_invalid_backends           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_get_senders_catchup_time       | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_group                          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_gtt_attached_pids              | view  | omm   | 0 bytes    | {security_barrier=true}          |
 pg_catalog | pg_gtt_relstats                   | view  | omm   | 0 bytes    | {security_barrier=true}          |
 pg_catalog | pg_gtt_stats                      | view  | omm   | 0 bytes    | {security_barrier=true}          |
 pg_catalog | pg_hashbucket                     | table | omm   | 8192 bytes |                                  |
 pg_catalog | pg_index                          | table | omm   | 80 kB      |                                  |
 pg_catalog | pg_indexes                        | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_inherits                       | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_job                            | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_job_proc                       | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_language                       | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_largeobject                    | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_largeobject_metadata           | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_locks                          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_namespace                      | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_node_env                       | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_object                         | table | omm   | 8192 bytes |                                  |
 pg_catalog | pg_obsscaninfo                    | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_opclass                        | table | omm   | 64 kB      |                                  |
 pg_catalog | pg_operator                       | table | omm   | 152 kB     |                                  |
 pg_catalog | pg_opfamily                       | table | omm   | 56 kB      |                                  |
 pg_catalog | pg_os_threads                     | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_partition                      | table | omm   | 8192 bytes |                                  |
 pg_catalog | pg_pltemplate                     | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_prepared_statements            | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_prepared_xacts                 | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_proc                           | table | omm   | 1056 kB    |                                  |
 pg_catalog | pg_range                          | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_replication_slots              | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_resource_pool                  | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_rewrite                        | table | omm   | 1424 kB    |                                  |
 pg_catalog | pg_rlspolicies                    | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_rlspolicy                      | table | omm   | 8192 bytes |                                  |
 pg_catalog | pg_roles                          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_rules                          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_running_xacts                  | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_seclabel                       | table | omm   | 8192 bytes |                                  |
 pg_catalog | pg_seclabels                      | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_session_iostat                 | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_session_wlmstat                | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_settings                       | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_shadow                         | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_shdepend                       | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_shdescription                  | table | omm   | 48 kB      |                                  |
 pg_catalog | pg_shseclabel                     | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_activity                  | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_activity_ng               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_all_indexes               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_all_tables                | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_bad_block                 | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_bgwriter                  | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_database                  | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_database_conflicts        | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_replication               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_sys_indexes               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_sys_tables                | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_user_functions            | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_user_indexes              | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_user_tables               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_xact_all_tables           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_xact_sys_tables           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_xact_user_functions       | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_stat_xact_user_tables          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statio_all_indexes             | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statio_all_sequences           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statio_all_tables              | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statio_sys_indexes             | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statio_sys_sequences           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statio_sys_tables              | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statio_user_indexes            | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statio_user_sequences          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statio_user_tables             | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_statistic                      | table | omm   | 264 kB     |                                  |
 pg_catalog | pg_statistic_ext                  | table | omm   | 8192 bytes |                                  |
 pg_catalog | pg_stats                          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_synonym                        | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_tables                         | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_tablespace                     | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_tde_info                       | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_thread_wait_status             | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_timezone_abbrevs               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_timezone_names                 | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_total_memory_detail            | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_total_user_resource_info       | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_total_user_resource_info_oid   | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_trigger                        | table | omm   | 16 kB      |                                  |
 pg_catalog | pg_ts_config                      | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_ts_config_map                  | table | omm   | 48 kB      |                                  |
 pg_catalog | pg_ts_dict                        | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_ts_parser                      | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_ts_template                    | table | omm   | 40 kB      |                                  |
 pg_catalog | pg_type                           | table | omm   | 184 kB     |                                  |
 pg_catalog | pg_user                           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_user_mapping                   | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_user_mappings                  | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_user_status                    | table | omm   | 0 bytes    |                                  |
 pg_catalog | pg_variable_info                  | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_views                          | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_wlm_statistics                 | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pg_workload_group                 | table | omm   | 40 kB      |                                  |
 pg_catalog | pgxc_class                        | table | omm   | 8192 bytes |                                  |
 pg_catalog | pgxc_group                        | table | omm   | 8192 bytes |                                  |
 pg_catalog | pgxc_node                         | table | omm   | 0 bytes    |                                  |
 pg_catalog | pgxc_prepared_xacts               | view  | omm   | 0 bytes    |                                  |
 pg_catalog | pgxc_slice                        | table | omm   | 0 bytes    |                                  |
 pg_catalog | pgxc_thread_wait_status           | view  | omm   | 0 bytes    |                                  |
 pg_catalog | plan_table                        | view  | omm   | 0 bytes    |                                  |
 pg_catalog | plan_table_data                   | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 pg_catalog | statement_history                 | table | omm   | 16 kB      | {orientation=row,compression=no} |
 pg_catalog | streaming_cont_query              | table | omm   | 0 bytes    |                                  |
 pg_catalog | streaming_reaper_status           | table | omm   | 0 bytes    |                                  |
 pg_catalog | streaming_stream                  | table | omm   | 0 bytes    |                                  |
 pg_catalog | sys_dummy                         | view  | omm   | 0 bytes    |                                  |
 public     | bank_card                         | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 public     | client                            | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 public     | finances_product                  | table | omm   | 16 kB      | {orientation=row,compression=no} |
 public     | fund                              | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 public     | insurance                         | table | omm   | 8192 bytes | {orientation=row,compression=no} |
 public     | property                          | table | omm   | 8192 bytes | {orientation=row,compression=no} |
(232 rows)
```
