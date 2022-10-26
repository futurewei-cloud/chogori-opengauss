mkdir -p /opt/opengauss
cp -r /build/install/usr/local/* /opt/opengauss/
cp -r /build/simpleInstall /opt/opengauss/
chown omm.dbgrp -R /opt/opengauss
su - omm
