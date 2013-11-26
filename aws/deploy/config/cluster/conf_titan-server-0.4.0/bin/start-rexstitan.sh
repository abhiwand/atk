#!/usr/bin/env bash
# Modelled after $HBASE_HOME/bin/start-hbase.sh.
# Start titan as rexster server

source `dirname $0`/rexstitan-env.sh

if [ -f $pid ]; then
	echo "$name is already running at `cat $pid`! If this is not the case, \
consider removing the PID file ($pid) and retry."
	exit 1
fi

# Java
JAVA="java -server"

# Class path
CP=$( echo `dirname $0`/../lib/*.jar . | sed 's/ /:/g')
CP=$CP:$(find -L `dirname $0`/../ext/ -name "*.jar" | tr '\n' ':')

# Memory profile
JAVA_OPTIONS="-Xms32m -Xmx512m"

# Logging
JAVA_LOGOPTS="-Dlog4j.configuration=file:$log4j -Drexstitan.log.dir=../logs -Drexstitan.log.file=$logbase.log"

# Rexster
REXTER_OPTS="-s -c $rexcfg" 

# - add rotate log
# - add niceness
command="$JAVA $JAVA_OPTIONS -cp $CP com.tinkerpop.rexster.Application $REXTER_OPTS"
echo starting $command, logging to $logout
echo "`date` Starting $command on `hostname`" >> $loglog
nohup $command  > "$logout" 2>&1 < /dev/null &
echo $! > $pid
sleep 1; 
head "$logout"
