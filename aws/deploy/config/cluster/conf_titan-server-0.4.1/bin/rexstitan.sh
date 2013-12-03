#!/usr/bin/env bash
# Modelled after $HBASE_HOME/bin/start-hbase.sh.
# Start titan as rexster server

source `dirname $0`/rexstitan-env.sh

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
$JAVA ${JAVA_OPTIONS} ${JAVA_LOGOPTS} -cp $CP com.tinkerpop.rexster.Application ${REXTER_OPTS}
# Find Java
if [ "${JAVA_HOME}" = "" ] ; then
     echo "JAVA_HOME is not set!"
     exit 1
fi
