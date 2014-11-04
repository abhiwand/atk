#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo $DIR

if [ "$DIR/stage" != "" ]; then
	rm -rf $DIR/stage
fi

if [ -e /usr/lib/intelanalytics/rest-server/lib/logback-classic-1.1.1.jar ]; then
    LOGBACK_JARS=/usr/lib/intelanalytics/rest-server/lib/logback-classic-1.1.1.jar:/usr/lib/intelanalytics/rest-server/lib/logback-core-1.1.1.jar
elif [ -e ~/.m2/repository/ch/qos/logback/logback-classic/1.1.1/logback-classic-1.1.1.jar ]; then
    LOGBACK_JARS=~/.m2/repository/ch/qos/logback/logback-classic/1.1.1/logback-classic-1.1.1.jar:~/.m2/repository/ch/qos/logback/logback-core/1.1.1/logback-core-1.1.1.jar
else
    echo "ERROR: could not find logback jars"
    exit 2
fi

CONFDIR=$DIR/../api-server/src/main/resources:$DIR/../engine/src/main/resources:$DIR/../conf/application.conf:$LOGBACK_JARS

if [[ -f $DIR/../launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../launcher/target/launcher.jar
fi

CONFIG_CLASSPATH="/etc/hbase/conf:/etc/hadoop/conf"

if [[ -n "$EXTRA_CONF" ]]
 then
    CONF="$EXTRA_CONF:$CONFDIR:$CONFIG_CLASSPATH"
else
    CONF="$CONFDIR:CONFIG_CLASSPATH"
fi

pushd $DIR/..
pwd

export HOSTNAME=`hostname`


echo java $@ -XX:MaxPermSize=256m -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication
java $@ -XX:MaxPermSize=256m -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication

popd
