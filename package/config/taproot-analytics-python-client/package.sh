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

#releaseNumber=$(echo $BRANCH | awk '/release_[0-9.]*$/{print substr($0, match($0,"[0-9.]*$"))}')
#if [ "$releaseNumber" != "" ]; then

#    python -m compileall ../python-client/taproot/

#    mkdir -p ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/rest
#    cp ../python-client/taproot/rest/config.py ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/rest/config.py
#    log "remove py files"
#    #find ../python-client/taproot -name *.py -type f -delete
#    ls -l ../python-client/taproot/core
#    cp -Rv  ../python-client/taproot/* ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/

#else
    log "regular package"
    cp -Rv  ../python-client/taproot/* ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/
    log "delete pyc files"
    find ../python-client/taproot -name *.pyc -type f -delete

#fi

cp -Rv  ../python-client/cmdgen.py ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/

cp -Rv ../doc/build/html ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/doc/html/

createArchive $packageName
