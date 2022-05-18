#!/usr/bin/bash
set -e

mkdir -p /opt/opengauss
export GAUSSHOME=/opt/opengauss
./configure --gcc-version=8.5.0 CC=g++ CFLAGS='-O2 -g3' --prefix=$GAUSSHOME --3rd=/tmp/openGauss-third_party_binarylibs --enable-mot --enable-thread-safety --without-readline --without-zlib
make -j4
make install
cp -r ./simpleInstall /opt/opengauss/
mkdir -p /opt/opengauss/data
chown omm.dbgrp -R /opt/opengauss
