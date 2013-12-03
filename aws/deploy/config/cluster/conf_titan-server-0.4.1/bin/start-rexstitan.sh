#!/usr/bin/env bash
# Modelled after $HBASE_HOME/bin/start-hbase.sh.
# Start titan as rexster server

source `dirname $0`/rexstitan-env.sh

# is it running?
if [ -f $pid ]; then
	echo "$name is already running at `cat $pid`! If this is not the case, \
consider removing the PID file ($pid) and retry."
	exit 1
fi

# titan-server dir
pushd $rexdir 

# Java
JAVA="java -server"

# Class path
CP=$( echo lib/*.jar . | sed 's/ /:/g')
CP=$CP:$(find -L ext/ -name "*.jar" | tr '\n' ':')

# Memory profile
JAVA_OPTIONS="-Xms32m -Xmx512m"

# Logging
JAVA_LOGOPTS="-Dlog4j.configuration=file:$log4j -Drexstitan.log.dir=${logdir} -Drexstitan.log.file=${logbase}.log"

# Rexster
REXTER_OPTS="-s -c $rexcfg" 

command="$JAVA $JAVA_OPTIONS $JAVA_LOGOPTS -cp $CP com.tinkerpop.rexster.Application $REXTER_OPTS"
echo -n "Starting $name, logging to $loglog"
echo "`date` Starting $command on `hostname`" >> $loglog
nohup $command  > "$logout" 2>&1 < /dev/null &
echo $! > $pid
sleep 1; 
head "$logout"

# get back
popd
