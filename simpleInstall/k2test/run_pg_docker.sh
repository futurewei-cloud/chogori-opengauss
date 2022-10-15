#!/bin/bash
REMOTE=${REMOTE:=platform}
cd /opt/opengauss/simpleInstall
# Change default nework to docker compose network
sed -i -e 's|172.17.0.1|'${REMOTE}'|g' k2config_pgrun.json
# TODO: Install demo databy by changing to `echo yes` when k2 initdb is fully working
echo no |. pg_run.sh -w Test3456
