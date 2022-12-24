#!/bin/bash
set -m
set -e
#PROTO="auto-rrdma+k2rpc"
PROTO="tcp+k2rpc"

CPODIR=${CPODIR:=/tmp/___cpo_integ_test}
rm -rf ${CPODIR}
DEFAULT_EPS=("${PROTO}://0.0.0.0:10000" "${PROTO}://0.0.0.0:10001" "${PROTO}://0.0.0.0:10002" "${PROTO}://0.0.0.0:10003" "${PROTO}://0.0.0.0:10004" "${PROTO}://0.0.0.0:10005" "${PROTO}://0.0.0.0:10006" "${PROTO}://0.0.0.0:10007")
EPS=( ${EPS:=${DEFAULT_EPS[@]}} )
NUMCORES=${NUMCORES:=`nproc`}
# core on which to run the TSO poller thread. Pick 4 if we have that many, or the highest-available otherwise
#TSO_POLLER_CORE=$(( 5 > $NUMCORES ? $NUMCORES-1 : 4 ))
# poll on random core
TSO_POLLER_CORE=-1
PERSISTENCE=${PERSISTENCE:=${PROTO}://0.0.0.0:12001}
CPO=${CPO:=${PROTO}://0.0.0.0:9000}
TSO=${TSO:=${PROTO}://0.0.0.0:13000}
COMMON_ARGS=${COMMON_ARGS:="--reactor-backend=linux-aio --enable_tx_checksum false --thread-affinity false --hugepages"}
HTTP=${HTTP:=${PROTO}://0.0.0.0:20000}
HTTPMEM=${HTTPMEM:=8G}
PROMETHEUS_PORT_START=${PROMETHEUS_PORT_START:=63000}

# start CPO
cpo_main ${COMMON_ARGS} -c1 --tcp_endpoints ${CPO}  --data_dir ${CPODIR} --prometheus_port $PROMETHEUS_PORT_START --assignment_timeout=1s --cpo.tso_assignment_timeout=100ms --cpo.assignment_base_backoff=100ms --txn_heartbeat_deadline=1s --nodepool_endpoints ${EPS[@]} --tso_endpoints ${TSO} --tso_error_bound=100us --persistence_endpoints ${PERSISTENCE} &
cpo_child_pid=$!

# start nodepool
nodepool ${COMMON_ARGS} -c${#EPS[@]} --tcp_endpoints ${EPS[@]} --k23si_persistence_endpoint ${PERSISTENCE} --prometheus_port $(($PROMETHEUS_PORT_START+1)) --memory=2G --partition_request_timeout=6s &
nodepool_child_pid=$!

# start persistence
persistence ${COMMON_ARGS} -c1 --tcp_endpoints ${PERSISTENCE} --prometheus_port $(($PROMETHEUS_PORT_START+2)) &
persistence_child_pid=$!

# start tso
tso ${COMMON_ARGS} -c1 --tcp_endpoints ${TSO} --prometheus_port $(($PROMETHEUS_PORT_START+3)) --tso.clock_poller_cpu=${TSO_POLLER_CORE} &
tso_child_pid=$!

# start http proxy
http_proxy ${COMMON_ARGS} -c1 --tcp_endpoints ${HTTP} --memory=${HTTPMEM} --cpo ${CPO} --httpproxy_txn_timeout=1h --httpproxy_expiry_timer_interval=50ms --log_level=Info k2::httpproxy=Info k2::cpo_client=Info --prometheus_port $(($PROMETHEUS_PORT_START+4)) --partition_request_timeout=100s &
http_child_pid=$!

function finish {
  echo "Cleaning up..."
  rv=$?
  # cleanup code
  rm -rf ${CPODIR}

  kill ${nodepool_child_pid}
  echo "Waiting for nodepool child pid: ${nodepool_child_pid}"
  wait ${nodepool_child_pid}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${persistence_child_pid}
  echo "Waiting for persistence child pid: ${persistence_child_pid}"
  wait ${persistence_child_pid}

  kill ${tso_child_pid}
  echo "Waiting for tso child pid: ${tso_child_pid}"
  wait ${tso_child_pid}

  kill ${http_child_pid}
  echo "Waiting for http child pid: ${http_child_pid}"
  wait ${http_child_pid}

  echo ">>>> ${0} finished with code ${rv}"
}
sleep 1
echo ">>>> Done setting up cluster"

trap finish EXIT

if [ -z "$1" ]
then
    echo ">>>> Press CTRL+C to exit..."
    sleep infinity
else
    echo ">>>> Running" "${1}" "${@:2}"
    eval "${1}" "${@:2}"
fi
