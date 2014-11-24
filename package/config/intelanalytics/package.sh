#!/bin/bash
workDir=$(pwd)
baseDir=${workDir##*/}
gitRoot="."
pwd
$baseDir

if [ "$baseDir" == "package" ]; then
	source common.sh
	gitRoot=".."
	else
	source package/common.sh
	gitRoot="."
fi
echo $gitRoot

packageName=$1
VERSION=$VERSION
BUILD_DIR=$BUILD_DIR

echo $packageName
echo $VERSION
echo $BUILD_DIR

pwd
echo $SCRIPTPATH

log "packageName: $packageName"
#call package.sh for rest-server
package intelanalytics-rest-server
packageName=$1
cp ${BUILD_DIR}/etc/intelanalytics/rest-server/parcel.conf.tpl  ${BUILD_DIR}/etc/intelanalytics/rest-server/application.conf

log "packageName: $packageName"
#call package.sh for rest server
package intelanalytics-python-rest-client
packageName=$1

log "packageName: $packageName"
#call package.sh for spark-deps
package intelanalytics-spark-deps
packageName=$1


#call package.sh for client
log "packageName: $packageName"
createArchive $packageName
