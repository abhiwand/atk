#!/bin/bash

CURRENT_DIR=`pwd`


if [ ! -d /var/log/cassandra ]; then
   sudo mkdir -p /var/log/cassandra
   sudo chown -R `whoami` /var/log/cassandra
fi

if [ ! -d /var/lib/cassandra ]; then
    sudo mkdir -p /var/lib/cassandra
    sudo chown -R `whoami` /var/lib/cassandra
fi


echo "start cassandra"
cd ~/Downloads
cassandra >& cassandra.log

RESULT=`tail -n 1 cassandra.log |  grep "Startup completed" | wc -l`
if [ $RESULT = 1 ] ; then
   echo "cassandra works fine"
else
   echo "cassandra does not work"
fi

echo "stop cassandra"
pkill -f CassandraDaemon

#http://wiki.apache.org/cassandra/GettingStarted
cd $CURRENT_DIR
