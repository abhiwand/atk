#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo $DIR

if [ "$DIR/stage" != "" ]; then
	rm -rf $DIR/stage
fi


#CONFDIR=$DIR/../conf
CONFDIR=$DIR/../api-server/src/main/resources:$DIR/../engine/src/main/resources:$DIR/../conf/application.conf

if [[ -f $DIR/../launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../launcher/target/launcher.jar
fi



HBASE_CLASSPATH=`hbase classpath`

if [[ -n "$EXTRA_CONF" ]]
 then
    CONF="$EXTRA_CONF:$CONFDIR:$HBASE_CLASSPATH"
else
    CONF="$CONFDIR:$HBASE_CLASSPATH"
fi

pushd $DIR/..
pwd                             i

export HOSTNAME=`hostname`


echo java $@ -XX:MaxPermSize=256m -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot engine-spark com.intel.intelanalytics.engine.spark.CommandDumper
java $@ -XX:MaxPermSize=256m -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot engine-spark com.intel.intelanalytics.engine.spark.CommandDumper

popd