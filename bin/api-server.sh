#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo $DIR

#jars="engine-spark*-jar*.jar api-server-*jar*.jar engine-jar*.jar interfaces*-jar*.jar "
jars="engine-spark.jar api-server.jar engine.jar interfaces.jar "

if [ "$DIR/stage" != "" ]; then
	rm -rf $DIR/stage
fi

mkdir -p $DIR/stage/lib

pushd $DIR/..
for jar in $jars
do 
	jarPath=$(find .  -path ./package -prune -o -name $jar -print )
	echo $jarPath
	cp -v $jarPath $DIR/stage/lib/
	#oldName=`basename $jarPath`
	#newName=`basename $jarPath | sed "s/-jar-with-dependencies//g"`
	#echo $oldName
	#echo $newName
	#mv $DIR/stage/lib/$oldName $DIR/stage/lib/$newName
done
	jarPath=$(find . -path ./package -prune -o -name launcher.jar -print)
	echo $jarPath
	cp -v $jarPath $DIR/stage/launcher.jar
	cp -v $jarPath $DIR/stage/lib/launcher.jar

popd

if [[ -f $DIR/stage/launcher.jar ]]; then
    LAUNCHER=$DIR/stage/launcher.jar
    CONFDIR=$DIR/../api-server/src/main/resources
else
	exit 1
fi


if [[ -n "$EXTRA_CONF" ]]
 then
    CONF="$EXTRA_CONF:$CONFDIR"
else
    CONF="$CONFDIR"
fi
pushd $DIR/stage
pwd

echo java $@ -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ServiceApplication
java $@ -cp "$CONF:$LAUNCHER" com.intel.intelanalytics.component.Boot api-server com.intel.intelanalytics.service.ServiceApplication

popd