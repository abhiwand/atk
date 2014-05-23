#!/bin/bash
set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo $DIR

pushd $DIR/..
jars="engine-spark*-with-dependencies.jar api-server*-with-dependencies.jar engine-0*-with-dependencies.jar interfaces*-with-dependencies.jar "

if [ "$DIR/stage" != "" ]; then
	rm -rf $DIR/stage
fi

mkdir -p $DIR/stage/lib

for jar in $jars
do 
	jarPath=$(find .  $DIR/../package -path $DIR -prune -o -name $jar -print )
	echo $jarPath
	cp -v $jarPath $DIR/stage/lib/
done
	jarPath=$(find . -path $DIR/../package -path $DIR -prune -o -name launcher*-with-dependencies.jar -print)
	echo $jarPath
	cp -v $jarPath $DIR/stage
	cp -v $jarPath $DIR/stage/lib/


if [[ -f $DIR/stage/launcher-0.8.0-SNAPSHOT-jar-with-dependencies.jar ]]
then
    LAUNCHER=$DIR/stage/launcher-0.8.0-SNAPSHOT-jar-with-dependencies.jar
    CONFDIR=$DIR/../api-server/src/main/resources
else
    echo "else"
fi

if [[ -n "$EXTRA_CONF" ]]
 then
    CONF="$EXTRA_CONF:$CONFDIR"
else
    CONF="$CONFDIR"
fi

java $@ -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ServiceApplication
