Manaual Testing
==============
Before manual testing make sure chogori-platform and chogori-opengauss repos are synced to latest and
fully built by runing `k2build make -C k2b` and for chogori-opengauss `make install`. Then execute the following steps

1. Open a terminal for `chogori-platform`
    1. Go to `chogori-platform` folder
    2. Run
```bash
docker run -it -p 30000:30000 --rm k2-bvu-10001.usrd.futurewei.com/k2runner  run_k2_cluster.sh
```

2. Open another terminal for `chogori-opengauss`
  1. `cd` to `chogori-opengauss` folder
  2. Run
    ```bash
     k2build
    ```
    2. It will create a container for `chogori-opengauss`. Inside the container run
    ```bash
    /build/simpleInstall/k2test/setup.sh
    ````
    3. Run initdb as omm user:
    
    ```bash
    su - omm
    cd /opt/opengauss/simpleInstall
    sh ./pg_run.sh  -w Test3456 
    ```

6. If You need to rerun, do following instead of doing all the above steps.
   1. Go to chogori-platform terminal and press ^C, it will exit the test server.
 Then re-run the docker command`
   2. In the chogori-opengauss container re-run `sh ./pg_run.sh -w Test3456`

Docker compose
=============

Run:

```bash
cd simpleInstall/k2test/buildtest
docker-compose up
# Check errors and exit and cleanup
^C
docker-compose down

```