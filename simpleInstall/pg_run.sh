#!/bin/bash

# Before running
# Run following in another terminal
# cd chogori-platform
#  DARGS='-p 30000:30000' k2build
# cd test/integration
# in test_http_client.sh remove last line that runs test and put a wait
# Run ./test_http_client
# After that k2 system can be accessed by port 30000

export SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# export LD_LIBRARY_PATH=$SCRIPT_DIR/../build/src/k2/connector/common/:$SCRIPT_DIR/../build/src/k2/connector/entities/:$SCRIPT_DIR/../src/k2/postgres/lib
export K2PG_ENABLED_IN_POSTGRES=1
export K2PG_TRANSACTIONS_ENABLED=1
export K2PG_ALLOW_RUNNING_AS_ANY_USER=1
export PG_NO_RESTART_ALL_CHILDREN_ON_CRASH=1
export K2_CONFIG_FILE=$SCRIPT_DIR/k2config_pgrun.json

. ./install.sh "$@"
