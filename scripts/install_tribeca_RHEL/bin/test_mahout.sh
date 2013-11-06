#!/bin/bash

MAHOUT_HOME=$1

#mahout test case
pushd ../test
NOT_STARTED=`jps |grep DataNode | wc -l`
if [ $NOT_STARTED = 0 ]; then
   echo "start hadoop"
   start-all.sh
   jps
   hadoop dfsadmin -safemode leave
   hadoop dfsadmin -report
fi


HAS_FILE=`hadoop fs -ls | grep  testdata|wc -l`
if [ $HAS_FILE = 0 ]; then
    hadoop fs -mkdir testdata
fi
    hadoop fs -put synthetic_control.data testdata

JOB="org.apache.mahout.clustering.syntheticcontrol.kmeans.Job"
echo "mahout $JOB"
mahout $JOB >& mahout.log

RESULT=`grep "ClusterDumper" mahout.log| wc -l`
if [ $RESULT = 1 ]; then
   echo  "mahout works fine"
else
   echo  "mahout does not work"
fi

#rm -rf mahout.log
hadoop fs -rmr -skipTrash testdata
hadoop fs -rmr -skipTrash output/cluster*


if [ $NOT_STARTED ]; then
   echo "stop hadoop"
   stop-all.sh
fi
echo " "
popd
