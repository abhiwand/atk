#!/bin/bash
workDir=$(pwd)
baseDir=${workDir##*/}
gitRoot="."
if [ "$baseDir" == "package" ]; then
	source common.sh
	gitRoot=".."
	else
	source package/common.sh
	gitRoot="."
fi

packageName=$1
VERSION=$VERSION
BUILD_DIR=$BUILD_DIR

echo $packageName
echo $VERSION
echo $BUILD_DIR

echo "$SCRIPTPATH"

pwd

mkdir -p  ${BUILD_DIR}/etc/intelanalytics/rest-server
mkdir -p  ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/lib

#copy example scripts
mkdir -p ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/examples/
cp -Rv ../python-examples/end-user/* ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/examples/

if [ -d /home/agent/datasets ]; then
    #copy datasets from agent home if it exists into the rpm tar.gz source
    cp -Rv /home/agent/datasets ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/examples
fi


cp -v ../conf/examples/application.conf.tpl ${BUILD_DIR}/etc/intelanalytics/rest-server
cp -v ../conf/examples/parcel.conf.tpl      ${BUILD_DIR}/etc/intelanalytics/rest-server
cp -v ../conf/examples/application.conf.single-system.tpl ${BUILD_DIR}/etc/intelanalytics/rest-server

pushd $SCRIPTPATH
    cp -Rv assets/* ${BUILD_DIR}
popd

#excluded jars are now combined in deploy.jar
#engine-spark.jar igiraph-titan.jar graphon.jar
jars=" api-server.jar  engine.jar  interfaces.jar  deploy.jar"

pushd ..
for jar in $jars
do
	jarPath=$(find .  -path ./package -prune -o -name $jar -print )
	echo $jarPath
	cp -v $jarPath ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/lib/

done

jarPath=$(find .  -path ./package -prune -o -name launcher.jar -print)

echo $jarPath
#enable this to copy the regular launcher.jar to the correct place
cp -v $jarPath ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/launcher.jar

#jarPath=$(find /root/.m2/repository/ch/qos/logback/ -name "logback-classic-1.1.1.jar")
#cp -v $jarPath ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/lib/
#jarPath=$(find /root/.m2/repository/ch/qos/logback/ -name "logback-core-1.1.1.jar")
#cp -v $jarPath ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/lib/


popd

log "createArchive $packageName"
createArchive $packageName
