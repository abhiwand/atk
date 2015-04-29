#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"


if [[ -f $DIR/../launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../launcher/target/launcher.jar:.
fi

pushd $DIR/..
pwd

export HOSTNAME=`hostname`

echo "##########################################################"
echo Vars to fake the CF VCAPs

# use CF_HOME and read the config.json directly
if [ -z "$CF_HOME" ]; then
    export CF_HOME=~
fi

cf_config_file=$CF_HOME/.cf/config.json

if [ -f $cf_config_file ];
then
    cf_json=`cat $cf_config_file`

    temp=`echo $cf_json | jq '.Target'`
    temp="${temp%\"}"  # strip quotes
    temp="${temp#\"}"
    temp=`echo $temp | sed 's/^https/http/'`  # most likely inside proxy, https won't work with spray
    export CC_URI=$temp

    temp=`echo $cf_json | jq '.UaaEndpoint'`
    temp="${temp%\"}"  # strip quotes
    temp="${temp#\"}"
    temp=`echo $temp | sed 's/^https/http/'`  # most likely inside proxy, https won't work with spray
    export UAA_URI=$temp

    # assume APP_SPACE is current space
    temp=`echo $cf_json | jq '.SpaceFields.Guid'`
    temp="${temp%\"}"  # strip quotes
    temp="${temp#\"}"
    export APP_SPACE=$temp
else
    export CC_URI=INVALID_CC_URI
    export UAA_URI=INVALID_UAA_URI
    export APP_SPACE=INVALID_APP_SPACE
fi

echo CC_URI=$CC_URI
echo UAA_URI=$UAA_URI
echo APP_SPACE=$APP_SPACE
echo "##########################################################"

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

PROXY_GOO="-Dhttp.proxyHost=proxy.jf.intel.com -Dhttp.proxyPort=911 -Dhttps.proxyHost=proxy.jf.intel.com -Dhttps.proxyPort=912"
echo java $@ -XX:MaxPermSize=256m $PROXY_GOO -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot api-server
java $@ -XX:MaxPermSize=256m $PROXY_GOO -cp "$LAUNCHER" com.intel.intelanalytics.component.Boot api-server

popd
