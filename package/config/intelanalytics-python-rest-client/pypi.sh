#!/bin/bash
source common.sh

log "Build pypi package"

packageName=$1
tarFile=$2
TAR_FILE=$tarFile
version=$3

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

pushd $SCRIPTPATH

#create directory
mkdir -p intelanalytics/intelanalytics


tar -xvf $tarFile -C intelanalytics/

cp -Rv intelanalytics/usr/lib/intelanalytics/rest-client/python/* intelanalytics/intelanalytics

rm -rf usr

#copy assest files
cp -Rv assets/* intelanalytics/

pushd intelanalytics

python setup.py sdist

popd

popd