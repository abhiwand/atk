#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo $DIR

if [ "$DIR/stage" != "" ]; then
	rm -rf $DIR/stage
fi


CONFDIR=$DIR/../api-server/src/main/resources:$DIR/../engine/src/main/resources

if [[ -f $DIR/../launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../launcher/target/launcher.jar
fi


if [[ -n "$EXTRA_CONF" ]]
 then
    CONF="$EXTRA_CONF:$CONFDIR"
else
    CONF="$CONFDIR"
fi

pushd $DIR/..
pwd

echo java $@ -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ServiceApplication
java $@ -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ServiceApplication

popd