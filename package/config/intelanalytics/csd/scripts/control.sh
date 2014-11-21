#!/bin/bash

env

function log {
timestamp=$(date)
echo "$timestamp: $1" #stdout
echo "$timestamp: $1" 1>&2; #stderr
}


#exec >>/var/log/intelanalytics/rest-server/output.log 2>&1
case "$1" in
  start)
    log "restart intelanalytics start"
    exec su -c "java $IA_JVM_OPT -cp $CLASSPATH com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication" $IAUSER

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
