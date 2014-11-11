#!/bin/bash
set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo $DIR

CONFDIR=$DIR/../conf

LAUNCHER=$DIR/../launcher.jar

env

HBASE_CLASSPATH="wrong"
#HBASE_CLASSPATH=`hbase classpath`

if [[ -n "$EXTRA_CONF" ]]
then
CONF="$EXTRA_CONF:$CONFDIR:$HBASE_CLASSPATH"
else
CONF="$CONFDIR:$HBASE_CLASSPATH"
fi

pushd $DIR/..
pwd

export HOSTNAME=`hostname`

export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

echo java $@ -XX:MaxPermSize=256m -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication
java $@ -XX:MaxPermSize=256m -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ApiServiceApplication

popd