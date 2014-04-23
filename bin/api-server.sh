#!/bin/bash
set -o nounset
set -o errexit

DIR="$( cd "$( dirname "$0" )" && pwd )"

echo $DIR

if [[ -f $DIR/../launcher/target/scala-2.10/launcher.jar ]]
then
    LAUNCHER=$DIR/../launcher/target/scala-2.10/launcher.jar
    CONFDIR=$DIR/../api-server/src/main/resources
else
    LAUNCHER=$DIR/launcher.jar
    CONFDIR=$DIR/conf
fi

java $@ -cp $CONFDIR:$LAUNCHER com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ServiceApplication