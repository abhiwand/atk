#!/bin/bash
#
# This is a special version of the API Server start-up script for integration testing on a build machine
#

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "$NAME DIR=$DIR"

if [ "$DIR/stage" != "" ]; then
	rm -rf $DIR/stage
fi

# need the logback jars from somewhere
if [ -e /usr/lib/intelanalytics/rest-server/lib/logback-classic-1.1.1.jar ]; then
    LOGBACK_JARS=/usr/lib/intelanalytics/rest-server/lib/logback-classic-1.1.1.jar:/usr/lib/intelanalytics/rest-server/lib/logback-core-1.1.1.jar
elif [ -e ~/.m2/repository/ch/qos/logback/logback-classic/1.1.1/logback-classic-1.1.1.jar ]; then
    LOGBACK_JARS=~/.m2/repository/ch/qos/logback/logback-classic/1.1.1/logback-classic-1.1.1.jar:~/.m2/repository/ch/qos/logback/logback-core/1.1.1/logback-core-1.1.1.jar
elif [ -e $DIR/target/logback-deps/logback-classic-1.1.1.jar ]; then
     # Maven will copy jars here
    LOGBACK_JARS=$DIR/target/logback-deps/logback-classic-1.1.1.jar:$DIR/target/logback-deps/logback-core-1.1.1.jar
else
    echo "$NAME ERROR: could not find logback jars"
    exit 2
fi

CONFDIR=$DIR/conf:$DIR/../api-server/src/main/resources:$DIR/../engine/src/main/resources:$DIR/../conf/application.conf:$LOGBACK_JARS

if [[ -f $DIR/../launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../launcher/target/launcher.jar
fi

CONFIG_CLASSPATH="/etc/hbase/conf:/etc/hadoop/conf"

# EXTRA_CLASSPATH is not used in this script
CONF="$CONFDIR:$CONFIG_CLASSPATH"

pushd $DIR/..
pwd

export HOSTNAME=`hostname`

PORT=19099
echo "$NAME making sure port $PORT isn't already in use"
if netstat -atn | grep -q :$PORT
then
    echo "$NAME ERROR: Port $PORT is already in use!!! (it can take a little while for it to be released, we should switch to random port)"
    exit 2
else
    echo "$NAME Port $PORT is free"
fi

export TARGET_DIR=$DIR/target
export FS_ROOT=$TARGET_DIR/fs-root
LOG=$TARGET_DIR/api-server.log

mkdir -p $FS_ROOT

echo "$NAME remove old api-server.log and datasets from target dir"
rm -f $LOG $TARGET_DIR/datasets

echo "$NAME copying datasets to target"
cp -rp $DIR/datasets $FS_ROOT

echo "$NAME fs.root is $FS_ROOT"
echo "$NAME Api Server logging going to $LOG"

java $@ -XX:MaxPermSize=256m -cp "$CONF:$LAUNCHER" \
    -Dconfig.resource=integration-test.conf \
    -Dintel.analytics.engine.fs.root=file://$FS_ROOT \
    com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication > $LOG 2>&1 &

API_SERVER_PID=$!

echo $API_SERVER_PID > $TARGET_DIR/api-server.pid

popd
