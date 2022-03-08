## Build Docker Container image for OpenGauss

We target for OpenGauss stable version V2.1.0. To build OpenGauss from source, we need both the openGauss-server and its dependencies in project openGauss-third_party. To avoid building the third party dependencies from source, we could download the [binary](https://opengauss.obs.cn-south-1.myhuaweicloud.com/2.1.0/openGauss-third_party_binarylibs.tar.gz) and use it directly.

Unfortunately, openGauss did not provide a docker file for V2.1.0, we created a file for V2.1.0 at docker/dockerfiles/dockerfile to help build the openGauss source code.

The build steps:
- Go to dockerfile fold:
```bash
cd docker/dockerfiles/
```
- Build docker image opengauss-server-builder:
```bash
docker build -t opengauss-server-builder  - < dockerfile
```
- Run the container:
```bash
docker run --privileged -it opengauss-server-builder bash
```
- Go to /tmp/openGauss-server inside the docker
- Configure the openGauss server, for example, with this command:
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
