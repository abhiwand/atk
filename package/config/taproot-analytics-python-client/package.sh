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

mkdir -p  ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/doc

pushd $SCRIPTPATH
    cp -v requirements-linux.txt ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/
popd


log "regular package"
cp -Rv  ../python-client/taprootanalytics/* ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/
log "delete pyc files"
find ../python-client/taprootanalytics -name *.pyc -type f -delete

mkdir -p ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/examples/

cp -Rv ../python-client/examples/end-user/* ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/examples/

cp -Rv  ../python-client/cmdgen.py ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/

cp -Rv ../doc/build/html ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/doc/html/

createArchive $packageName
