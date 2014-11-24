#!/bin/bash

env

function log {
timestamp=$(date)
echo "==$timestamp: $1" #stdout
echo "==$timestamp: $1" 1>&2; #stderr
}

CLOUDERA_PARCEL_HOME="/opt/cloudera/parcels"

exec >>/var/log/intelanalytics/rest-server/output.log 2>&1
case "$1" in
  start)
    log "start intelanalytics start"
    pushd $ATK_LAUNCHER_DIR
    log `pwd`
    pwd
    ls -l
    echo java -XX:MaxPermSize=$ATK_MAX_HEAPSIZE $ATK_JVM_OPT -cp $ATK_CLASSPATH com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication
    exec  java -XX:MaxPermSize=$ATK_MAX_HEAPSIZE $ATK_JVM_OPT -cp $ATK_CLASSPATH com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication
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
