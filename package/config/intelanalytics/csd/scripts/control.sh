#!/bin/bash

env

echo "" > $ATK_TEMP/application.conf

function log {
timestamp=$(date)
echo "==$timestamp: $1" #stdout
}

function getConfig(){
    local service=$1
    local config_group=$2
    local config=$3
    python ${CONF_DIR}/scripts/config.py --host $ATK_CDH_HOST --port $ATK_CDH_PORT --username $ATK_CDH_USERNAME --password $ATK_CDH_PASSWORD --service "$service" --config-group "$config_group" --config "$config"
}

function getHostnames(){
    local service=$1
    local role=$2
    python ${CONF_DIR}/scripts/config.py --host $ATK_CDH_HOST --port $ATK_CDH_PORT --username $ATK_CDH_USERNAME --password $ATK_CDH_PASSWORD --service "$service" --role "$role" --hostnames yes
}

fs_root_host=$(getHostnames "HDFS" "NAMENODE" )
log "fs root host-${fs_root_host}"

fs_root_port=$(getConfig "HDFS" "hdfs-NAMENODE-BASE" "namenode_port" )
log "fs root port-${fs_root_port}"

echo "intel.analytics.engine.fs.root=\"hdfs://${fs_root_host}:${fs_root_port}/user/${ATK_USER}\"" >> $ATK_TEMP/application.conf

zookeeper_hosts=$(getHostnames "ZOOKEEPER" "SERVER" )
zHosts=""
for z in $zookeeper_hosts
do
    zHosts=$zHosts",$z"
done
zHosts=${zHosts#,}
log "zookeeper hosts-${zHosts}"

zookeeper_port=$(getConfig "ZOOKEEPER" "zookeeper-SERVER-BASE" "clientPort" )
log "zookeeper port-${zookeeper_port}"

echo "intel.analytics.engine.titan.load.storage.hostname=\"${zHosts}\"" >> $ATK_TEMP/application.conf
echo "intel.analytics.engine.titan.load.storage.port=${zookeeper_port}" >> $ATK_TEMP/application.conf

spark_master_host=$(getHostnames "SPARK" "SPARK_MASTER" )
log "spark master host-${spark_master_host}"

spark_master_port=$(getConfig "SPARK" "spark-SPARK_MASTER-BASE" "master_port" )
log "spark master port-${spark_master_port}"
echo "intel.analytics.engine.spark.master=\"spark://${spark_master_host}:${spark_master_port}\"" >> $ATK_TEMP/application.conf

spark_executor_mem=$(getConfig "SPARK" "spark-SPARK_WORKER-BASE" "executor_total_max_heapsize" )
log "spark exec mem-${spark_executor_mem}"
echo "intel.analytics.engine.spark.conf.properties.spark.executor.memory=\"${spark_executor_mem}\"" >> $ATK_TEMP/application.conf

log "set classpath"
python ${CONF_DIR}/scripts/config.py --host $ATK_CDH_HOST --port $ATK_CDH_PORT --username $ATK_CDH_USERNAME --password $ATK_CDH_PASSWORD --service "SPARK" --config-group "spark-SPARK_WORKER-BASE" --config "SPARK_WORKER_role_env_safety_valve" --classpath yes --classpath-lib "${ATK_SPARK_DEPS_DIR}/${ATK_SPARK_DEPS_JAR}"  --role "SPARK_MASTER"

echo "intel.analytics.engine.spark.python-worker-exec=\"${ATK_PYTHON}\"" >> $ATK_TEMP/application.conf
echo "intel.analytics.metastore.connection-postgresql.password=\"${ATK_POSTGRES_PASSWORD}\"" >> $ATK_TEMP/application.conf

if [ ! -z "$ATK_DEFAULT_TIMEOUT" ];then
    echo "intel.analytics.api.default-timeout=${ATK_DEFAULT_TIMEOUT}s" >> $ATK_TEMP/application.conf
    echo "intel.analytics.engine.default-timeout=${ATK_DEFAULT_TIMEOUT}" >> $ATK_TEMP/application.conf
    #minus one
    requestTimeout=$((ATK_DEFAULT_TIMEOUT - 1))
    echo "intel.analytics.api.request-timeout=${requestTimeout}s" >> $ATK_TEMP/application.conf
fi


sed -i "s|/var/log/intelanalytics/rest-server/application.log|${ATK_LOG}/application.log|g" ${ATK_CONFIG_DIR}/logback.xml
sed -i "s|/var/log/intelanalytics/rest-server/|${ATK_LOG}|g" ${ATK_PARCEL_HOME}/etc/logrotate.d/intelanalytics-rest-server

exec >> ${ATK_LOG}/output.log 2>&1
case "$1" in
  start)
    log "start intelanalytics start"
    pushd $ATK_LAUNCHER_DIR
    log `pwd`
    echo  java -XX:MaxPermSize=$ATK_MAX_HEAPSIZE $ATK_ADD_JVM_OPT -cp $ATK_TEMP:$ATK_CLASSPATH:$ATK_CLASSPATH_ADD com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication
    exec  java -XX:MaxPermSize=$ATK_MAX_HEAPSIZE $ATK_ADD_JVM_OPT -cp $ATK_TEMP:$ATK_CLASSPATH:$ATK_CLASSPATH_ADD com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication
    popd
    log "startted intelanalytics start"
	;;
#  stop)
#    echo "restart intelanalytics stop"
#	service intelanalytics stop
# ;;
#  restart)
#    echo "restart intelanalytics server"
#	service intelanalytics restart
#	;;
  *)
	log "Don't understand [$1]"
	exit 2
esac



  #or log by piping to logger
