#!/bin/bash
export GAUSSHOME=/opt/opengauss
export GS_CLUSTER_NAME=dbCluster
export GAUSSLOG=${GAUSSHOME}/logs
export PGDATA=${GAUSSHOME}/data/single_node
export PATH=${GAUSSHOME}/bin:${PATH}
export LD_LIBRARY_PATH=${GAUSSHOME}/lib:${LD_LIBRARY_PATH}
export K2PG_ENABLED_IN_POSTGRES=1
export K2PG_TRANSACTIONS_ENABLED=1
export K2PG_ALLOW_RUNNING_AS_ANY_USER=1
export PG_NO_RESTART_ALL_CHILDREN_ON_CRASH=1

cd ${GAUSSHOME}/simpleInstall/
export SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
export K2_CONFIG_FILE=${SCRIPT_DIR}/k2config_pgrun.json

#gdb --args gaussdb --single --localxid -F -O -c search_path=pg_catalog -c exit_on_error=true template1
gdb --args gaussdb -D /opt/opengauss/data/single_node --single_node
