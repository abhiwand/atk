#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"


# TODO: rename this file to command-dumper.sh

echo $DIR

if [[ -f $DIR/../misc/launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../misc/launcher/target/launcher.jar
fi

pushd $DIR/..
pwd                             i

export HOSTNAME=`hostname`

echo java $@ -XX:MaxPermSize=256m -Dconfig.trace=loads -cp "$LAUNCHER" com.intel.taproot.analytics.component.Boot engine-core com.intel.taproot.analytics.engine.CommandDumper
java $@ -XX:MaxPermSize=256m -Dconfig.trace=loads -cp "$LAUNCHER" com.intel.taproot.analytics.component.Boot engine-core com.intel.taproot.analytics.engine.CommandDumper

popd
