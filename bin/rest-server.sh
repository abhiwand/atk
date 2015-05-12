#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"


if [[ -f $DIR/../misc/launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../misc/launcher/target/launcher.jar:.
fi

pushd $DIR/..
pwd

export HOSTNAME=`hostname`
export YARN_CONF_DIR="/etc/hadoop/conf"

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

echo java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot rest-server
java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot rest-server

popd
