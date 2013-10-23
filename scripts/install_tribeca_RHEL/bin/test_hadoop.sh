#!/bin/bash

TEST_DIR=`pwd`/../test

NUM_JOB=`jps | wc -l`
if [ $NUM_JOB -gt 1 ]; then
   echo "stop hadoop"
   stop-all.sh
fi

echo "start hadoop"
start-all.sh
jps
hadoop dfsadmin -report
hadoop dfsadmin -safemode leave

hadoop fs -copyFromLocal $TEST_DIR/hadoop-test.txt test.txt
RESULT=`hadoop fs -ls | grep test.txt | wc -l`
if [ $RESULT = 1 ]; then
   echo "hadoop works fine"
else
   echo "hadoop does not work"
fi

hadoop fs -rmr -skipTrash test.txt

#if [ $NOT_STARTED ]; then
#   echo "stop hadoop"
#   stop-all.sh
#fi

