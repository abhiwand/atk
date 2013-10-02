#!/bin/bash

GIRAPH_HOME=$1
GIRAPH_JAR_VERSION=$2

#giraph test case
pushd ../test
NOT_STARTED=`jps |grep DataNode | wc -l`
if [ $NOT_STARTED = 0 ]; then
   echo "start hadoop"
   start-all.sh
   hadoop dfsadmin -safemode leave
fi


if hadoop fs -test -e giraph-test.txt ; then
     echo "Giraph test file  exists"
else
     echo "Copy Giraph test file"
    hadoop fs -copyFromLocal giraph-test.txt giraph-test.txt
fi

if hadoop fs -test -d test; then
   if hadoop fs -test -e test/pg; then
      hadoop fs -rmr -skipTrash hdfs:///user/`whoami`/test/pg
   fi
fi


JAR_FILE=$GIRAPH_HOME/giraph/giraph-examples/target/giraph-examples-$GIRAPH_JAR_VERSION-for-hadoop-0.20.203.0-jar-with-dependencies.jar
APP="org.apache.giraph.GiraphRunner org.apache.giraph.examples.SimplePageRankComputation"
IN_FORMAT="org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat"
IN_PATH=hdfs:///user/`whoami`/giraph-test.txt
OUT_FORMAT="org.apache.giraph.io.formats.IdWithValueTextOutputFormat"
OUT_PATH=hdfs:///user/`whoami`/test/pg
WORKER=2
M_COMPUTE=org.apache.giraph.examples.SimplePageRankComputation\$SimplePageRankMasterCompute
hadoop jar $JAR_FILE $APP -vif  $IN_FORMAT -vip $IN_PATH -vof $OUT_FORMAT -op $OUT_PATH -w $WORKER -mc $M_COMPUTE >& giraph.log

RESULT=`grep "Job complete" giraph.log | wc -l`
if [ $RESULT = 1 ]; then
   echo  "giraph works fine"
else
   echo  "giraph does not work"
fi

#rm -rf giraph.log
hadoop fs -rmr -skipTrash giraph-test.txt


if [ $NOT_STARTED ]; then
   echo "stop hadoop"
   stop-all.sh
fi
echo " "

popd
