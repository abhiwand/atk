#!/bin/bash
echo "Starting ATK startup script"

set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"
export KEYTAB=$DIR/../atk.keytab
export PRINCIPAL="atk-user@US-WEST-2.COMPUTE.INTERNAL"
#echo "Creating /opt/cloudera/parcels"
#mkdir -p /opt/cloudera/parcels
export ATK_CONF_DIR="$DIR/../conf"
export YARN_CONF_DIR=$ATK_CONF_DIR

#echo "Copying CDH installation"
#cp -R $DIR/../CDH /opt/cloudera/parcels/.

echo $DIR

#. ../env
LAUNCHER=$DIR/../launcher.jar
LAUNCHER=$DIR/../conf/logback.xml:$LAUNCHER
# set up jquery exectuable to parse environment variables
jq=$DIR/../jq
wget http://stedolan.github.io/jq/download/linux64/jq -O $jq
chmod +x $jq


echo "Setting environment variables"
export APP_NAME=$(echo $VCAP_APPLICATION | $jq -r .application_name)
#export SPARK_PUBLIC_DNS=$(echo $VCAP_APPLICATION | $jq -r .host_ip)
#export SPARK_DRIVER_HOST=$(echo $VCAP_APPLICATION | $jq -r .host_ip)
export SPARK_LOCAL_IP=$(hostname --ip-address)
#export SPARK_DRIVER_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.driver.host_port)
#export SPARK_FILESERVER_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.fileserver.host_port)
#export SPARK_BROADCAST_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.broadcast.host_port)
#export SPARK_REPL_CLASS_SERVER_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.replClassServer.host_port)
#export SPARK_BLOCK_MANAGER_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.blockManager.host_port)
#export SPARK_EXECUTOR_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.executor.host_port)
#export SPARK_UI_PORT=$(echo $VCAP_APPLICATION | $jq .extra_ports.ui.host_port)
#export SPARK_MASTER=$(echo $VCAP_SERVICES | $jq '.cdh | .[0].credentials.spark_master' | tr -d '"')
export FS_ROOT=$(echo $VCAP_SERVICES |  $jq '.cdh | .[0].credentials.hdfs_root' | tr -d '"')

#export ZOOKEEPER_HOST=$(echo $VCAP_SERVICES | $jq '.zookeeper | .[0].credentials.uri / ","  | tr -d '"')
export ZOOKEEPER_HOST=$(echo $VCAP_SERVICES | $jq '.zookeeper | .[0].credentials.uri  / "," | map(. / ":" | .[0]) | join(",")'  | tr -d '"')
#temporary workaround
#export ZOOKEEPER_HOST=10.10.10.8
#export ZOOKEEPER_PORT=$(echo $VCAP_SERVICES | $jq '.cdh | .[0].credentials.zk_port' | tr -d '"')
export ZOOKEEPER_PORT=$(echo $VCAP_SERVICES | $jq '.zookeeper | .[0].credentials.uri / "," | .[0] / ":" | .[1])' | tr -d '"')
export PG_HOST=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.hostname' | tr -d '"')
export PG_PORT=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.port' | tr -d '"')
export PG_USER=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.username' | tr -d '"')
export PG_PASS=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.password' | tr -d '"')
export PG_DB=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.dbname' | tr -d '"')
export PG_URL=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.uri' | tr -d '"')
env

pushd $ATK_CONF_DIR
#download yarn, hdfs and hbase configs
for url_suffix in "yarn_config" "hdfs_config" "hbase_config"
do
  conf_url=$(echo $VCAP_SERVICES |  $jq '.cdh | .[0].credentials.'$url_suffix | tr -d '"')
  zip_file=conf_$url_suffix.zip
  curl -X GET -H "content-type:application/json" $conf_url > zip_file
  unzip -o -j zip_file
  unlink zip_file
done
popd


pushd $DIR/..
pwd
export PWD=`pwd`

export HOSTNAME=$SPARK_DRIVER_HOST
#export SPARK_PUBLIC_DNS=$SPARK_DRIVER_HOST
export SPARK_LOCAL_IP=$SPARK_DRIVER_HOST
export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

jars="engine-spark.jar igiraph-titan.jar graphon.jar"
echo "Creating jar links"
for jar in $jars
do
if [ -f $DIR/../lib/$jar ]; then
   rm $DIR/../lib/$jar
 fi

 ln -s $DIR/../lib/deploy.jar $DIR/../lib/$jar
done

echo java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot rest-server
java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot rest-server

popd

