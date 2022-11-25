mkdir -p /opt/opengauss
cp -r /build/install/usr/local/* /opt/opengauss/
cp -r /build/simpleInstall /opt/opengauss/
chown omm.dbgrp -R /opt/opengauss
cd /opt/opengauss/simpleInstall
echo "run './pg_run.sh -w Test3456 2>&1 | tee /tmp/run.log' for initdb"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/lib:/lib
su omm
