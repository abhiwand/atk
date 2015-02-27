#!/bin/bash

set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

mkdir -p /opt/cloudera/parcels

cp -R $DIR/../CDH /opt/cloudera/parcels/.

echo $DIR

LAUNCHER=$DIR/../launcher.jar
jq=$DIR/../jq

export APP_NAME=$(echo $VCAP_APPLICATION | $jq -r .application_name)
export SPARK_PUBLIC_DNS=$(echo $VCAP_APPLICATION | $jq -r .host_ip)
#export SPARK_LOCAL_IP=0.0.0.0
export SPARK_DRIVER_HOST=$(echo $VCAP_APPLICATION | $jq -r .host_ip)
#export SPARK_DRIVER_HOST=$(hostname --ip-address)
export SPARK_DRIVER_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.driver.host_port)
export SPARK_FILESERVER_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.fileserver.host_port)
export SPARK_BROADCAST_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.broadcast.host_port)
export SPARK_REPL_CLASS_SERVER_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.replClassServer.host_port)
export SPARK_BLOCK_MANAGER_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.blockManager.host_port)
export SPARK_EXECUTOR_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.executor.host_port)
export SPARK_UI_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.ui.host_port)
export SPARK_MASTER=$(echo $VCAP_SERVICES | $jq '.cdh | .[0].credentials.spark_master' | tr -d '"')
export FS_ROOT=$(echo $VCAP_SERVICES |  $jq '.cdh | .[0].credentials.hdfs_root' | tr -d '"')
export ZOOKEEPER_HOST=$(echo $VCAP_SERVICES | $jq '.cdh | .[0].credentials.zk_host' | tr -d '"')
export ZOOKEEPER_PORT=$(echo $VCAP_SERVICES | $jq '.cdh | .[0].credentials.zk_port' | tr -d '"')
export PG_HOST=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.hostname' | tr -d '"')
export PG_PORT=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.port' | tr -d '"')
export PG_USER=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.username' | tr -d '"')
export PG_PASS=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.password' | tr -d '"')
export PG_DB=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.dbname' | tr -d '"')
export PG_URL=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.uri' | tr -d '"')
env

pushd $DIR/..
pwd

export HOSTNAME=$SPARK_DRIVER_HOST
#export SPARK_PUBLIC_DNS=$SPARK_DRIVER_HOST
export SPARK_LOCAL_IP=$SPARK_DRIVER_HOST
export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

echo java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot api-server
java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot api-server

popd

