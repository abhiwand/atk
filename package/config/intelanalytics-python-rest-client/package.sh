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

#mkdir -p  tarballs/$package/etc/intelanalytics/rest-server

mkdir -p  ${BUILD_DIR}/usr/lib/intelanalytics/rest-client/python/doc

pushd $SCRIPTPATH
    cp -v requirements-linux.txt ${BUILD_DIR}/usr/lib/intelanalytics/rest-client/python/
popd

releaseNumber=$(echo $BRANCH | awk '/release_[0-9.]*$/{print substr($0, match($0,"[0-9.]*$"))}')
if [ "$releaseNumber" != "" ]; then

    python -m compileall ../python-client/intelanalytics/

    mkdir -p ${BUILD_DIR}/usr/lib/intelanalytics/rest-client/python/rest
    cp ../python-client/intelanalytics/rest/config.py ${BUILD_DIR}/usr/lib/intelanalytics/rest-client/python/rest/config.py
    log "remove py files"
    find ../python-client/intelanalytics -name *.py -type f -delete
    ls -l ../python-client/intelanalytics/core
    cp -Rv  ../python-client/intelanalytics/* ${BUILD_DIR}/usr/lib/intelanalytics/rest-client/python/

else
    log "regular package"
    cp -Rv  ../python-client/intelanalytics/* ${BUILD_DIR}/usr/lib/intelanalytics/rest-client/python/
    log "delete pyc files"
    find ../python-client/intelanalytics -name *.pyc -type f -delete
    #find ../python-client/intelanalytics/ -type f -name "*.pyc" -exec rm -f {} \;
fi

cp -Rv  ../python-client/cmdgen.py ${BUILD_DIR}/usr/lib/intelanalytics/rest-client/

cp -Rv ../doc/build/html ${BUILD_DIR}/usr/lib/intelanalytics/rest-client/python/doc/html/

createArchive $packageName
