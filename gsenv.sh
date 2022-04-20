#!/bin/sh
# This script creates a local installation for opengauss, useful for testing
export GAUSSHOME=`realpath ./oghome`
mkdir -p ${GAUSSHOME}
chown omm ${GAUSSHOME}
chmod go-rwx ${GAUSSHOME}
INSTALL_PATH=`grep "^prefix" ./src/Makefile.global | cut -f 3 -d " "`
ln -sf ${INSTALL_PATH}/* ${GAUSSHOME}/
cd ${GAUSSHOME}

if [[ $# == 1 ]]
then
    cmd="gs_ctl $1 -D ${GAUSSHOME}/data/single_node -Z single_node"
    echo ">>> Running ${cmd} as user 'omm'"
    su omm -s/bin/bash -c "source ~/.bashrc && ${cmd} && /bin/bash"
else
    echo ">>> Running as user 'omm'"
    su omm
fi
