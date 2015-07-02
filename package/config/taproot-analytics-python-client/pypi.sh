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
mkdir -p taproot/taproot


tar -xvf $tarFile -C taproot/

cp -Rv taproot/usr/lib/taproot/analytics/python-client/* taproot/taproot

rm -rf usr

#copy assest files
cp -Rv assets/* taproot/
cp -v  requirements-windows.txt taproot/
cp -v  requirements-linux.txt taproot/

pushd taproot

sed -i "s/\#VERSION\#/${version}/g" setup.py
sed -i "s/\#BUILD_NUMBER\#/${BUILD_NUMBER}/g" setup.py

python setup.py sdist

popd

popd
