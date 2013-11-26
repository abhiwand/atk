#!/usr/bin/env bash
# Modelled after $HBASE_HOME/bin/start-hbase.sh.
# Start titan as rexster server

source `dirname $0`/rexstitan-env.sh

if [ ! -f $pid ]; then
	echo "$name is not running (pid file $pid not found)! "
	exit
fi

# kill -0 == see if the PID exists 
if kill -0 `cat $pid` > /dev/null 2>&1; then
	echo -n Stopping $name at process id `cat $pid`
	echo "`date` Terminating $name at `cat $pid`" >> $loglog
	kill `cat $pid` > /dev/null 2>&1
	while kill -0 `cat $pid` > /dev/null 2>&1; do
		echo -n "."
		sleep 1;
	done
	rm $pid
	echo
else
    retval=$?
	echo "Existing PID file ($pid) incidates $name is at `cat $pid`, \
but kill -0 `cat $pid` failed with status $retval. \
Consider removing the PID file ($pid)."

fi
