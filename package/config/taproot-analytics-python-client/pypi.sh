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
mkdir -p taprootanalytics/taprootanalytics


tar -xvf $tarFile -C taprootanalytics/

cp -Rv ${BUILD_DIR}/usr/lib/taproot/analytics/python-client/* taprootanalytics/taprootanalytics

rm -rf usr

#copy assest files
cp -Rv assets/* taprootanalytics/
cp -v  requirements-windows.txt taprootanalytics/
cp -v  requirements-linux.txt taprootanalytics/

pushd taprootanalytics

sed -i "s/\#VERSION\#/${version}/g" setup.py
sed -i "s/\#BUILD_NUMBER\#/${BUILD_NUMBER}/g" setup.py

python setup.py sdist

popd

popd
