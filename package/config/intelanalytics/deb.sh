#!/bin/bash
source common.sh
log "Build deb package"

packageName=$1
tarFile=$2
version=$3

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

log "copy tar.gz and rename for packaing"
cp $tarFile $SCRIPTPATH/${packageName}_${version}.orig.tar.gz
tar -xvf $tarFile -C $SCRIPTPATH/deb

pushd $SCRIPTPATH/deb

log "clean build dir"
mv debian ..
rm -rf *
mv ../debian . 


log "build deb package"
debuild -us -uc

popd $SCRIPTPATH/deb
