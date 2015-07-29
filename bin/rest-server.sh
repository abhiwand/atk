#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"


if [[ -f $DIR/../misc/launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../misc/launcher/target/launcher.jar:.
fi

if [ -f $DIR/../conf/application.conf ]; then
    LAUNCHER=$DIR/../conf/application.conf:$LAUNCHER
fi

pushd $DIR/..
pwd

export HOSTNAME=`hostname`
export YARN_CONF_DIR="/etc/hadoop/conf"

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

echo java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" com.intel.taproot.analytics.component.Boot rest-server
java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" com.intel.taproot.analytics.component.Boot rest-server

popd
