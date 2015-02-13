#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"


if [[ -f $DIR/../launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../launcher/target/launcher.jar
fi

pushd $DIR/..
pwd

export HOSTNAME=`hostname`

CONF=$DIR/../conf

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

echo java $@ -XX:MaxPermSize=256m -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server
java $@ -XX:MaxPermSize=256m -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server

popd
