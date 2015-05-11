#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo $DIR

if [[ -f $DIR/../launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../launcher/target/launcher.jar
fi

pushd $DIR/..
pwd                             i

export HOSTNAME=`hostname`

echo java $@ -XX:MaxPermSize=256m -Dconfig.trace=loads -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot engine com.intel.intelanalytics.engine.spark.CommandDumper
java $@ -XX:MaxPermSize=256m -Dconfig.trace=loads -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot engine com.intel.intelanalytics.engine.spark.CommandDumper

popd
