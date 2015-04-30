#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"


if [[ -f $DIR/../launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../launcher/target/launcher.jar:.
fi

pushd $DIR/..
pwd

export HOSTNAME=`hostname`

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

echo java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot test-server
java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot rest-server

popd
