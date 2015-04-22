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

mkdir -p  ${BUILD_DIR}/usr/lib/intelanalytics/graphbuilder/lib
jars="ispark-deps.jar"
for jar in $jars
do
	jarPath=$(find ..  -path ../package -prune -o -name $jar -print)
	echo $jarPath
    cp -v $jarPath ${BUILD_DIR}/usr/lib/intelanalytics/graphbuilder/lib/
done

#excluded jars are now combined in deploy.jar
#engine-spark.jar igiraph-titan.jar graphon.jar


mkdir -p  ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/lib
#jars="engine-spark.jar graphon.jar"
jars="deploy.jar"
for jar in $jars
do
	jarPath=$(find ..  -path ../package -prune -o -name $jar -print)
	echo $jarPath
    cp -v $jarPath ${BUILD_DIR}/usr/lib/intelanalytics/rest-server/lib
done


createArchive $packageName