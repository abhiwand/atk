#!/bin/bash



case "$1" in
  start)
    echo "restart intelanalytics start"
	service intelanalytics start
	;;
#  stop)
#    echo "restart intelanalytics stop"
#	service intelanalytics stop
# ;;
  restart)
    echo "restart intelanalytics server"
	service intelanalytics restart
	;;
  *)
	log "Don't understand [$1]"
	exit 2
esac


exec >>/var/log/intelanalytics/rest-server/output.log 2>&1
  #or log by piping to logger
exec su -c "java $IA_JVM_OPT -cp $CLASSPATH com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication" $IAUSER