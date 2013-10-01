#!/bin/bash

GRAPHLAB_HOME=$1
HADOOP_HOME=$2
HADOOP_VERSION=$3
pushd

cd ../test
echo `hostname` > hostfile

NOT_STARTED=`jps |grep DataNode | wc -l`
if [ $NOT_STARTED = 0 ]; then
   echo "start hadoop"
   start-all.sh
   jps
   hadoop dfsadmin -safemode leave
   hadoop dfsadmin -report
fi

HAS_FILE=`hadoop fs -ls graphlab-test.txt | wc -l`
if [ $HAS_FILE -eq 1 ]; then
    hadoop fs -rmr -skipTrash graphlab-test.txt
fi
hadoop fs -copyFromLocal graphlab-test.txt graphlab-test.txt

GLexe="$GRAPHLAB_HOME/graphlab/release/toolkits/graph_analytics/pagerank"
ingress=constrained_oblivious
GRAPH=hdfs:///user/`whoami`/graphlab-test.txt
NP=1
NCPU=1
FORMAT=tsv
ENGINE="async"
OUTPUT=hdfs:///user/`whoami`/gl-output-
MPIcmd=" -np $NP -hostfile hostfile env CLASSPATH=`$HADOOP_HOME/hadoop-$HADOOP_VERSION/bin/hadoop classpath`"
GLcmd=" --format=$FORMAT --graph=$GRAPH --engine=$ENGINE  --ncpus=$NCPU --graph_opts ingress=$ingress --saveprefix=$OUTPUT"
mpiexec $MPIcmd $GLexe $GLcmd >& graphlab.log
RESULT=`grep "Total rank" graphlab.log | awk '{print $3}' |wc -l`

if [ $RESULT -ne 0 ]; then
   echo "graphlab works fine"
else
   echo "graphlab does not work"
fi

hadoop fs -rmr -skipTrash graphlab-test.txt

if [ $NOT_STARTED ]; then
   echo "stop hadoop"
   stop-all.sh
fi

echo " "
popd
