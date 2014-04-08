#!/bin/bash
source common.sh
log "Build deb package"

packageName=$1
tarFile=$2
version=$3

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

log "copy tar.gz and rename for packaing"
expandTarDeb
#cp $tarFile $SCRIPTPATH/${packageName}_${version}.orig.tar.gz
#tar -xvf $tarFile -C $SCRIPTPATH/deb


SOURCE=$packageName
BUILD_DEPENDS="debhelper (>= 8.0.0)"
DESCRIPTION="i like to eat pie"
SUBJECT=$DESCRIPTION
log "create control file"
debControl > $SCRIPTPATH/deb/debian/control
log "create compat file"
debCompat >  $SCRIPTPATH/deb/debian/compat

log "create install file"
debInstall > $SCRIPTPATH/deb/debian/$packageName.install

pushd $SCRIPTPATH/deb

rm -rf debian/intelanalytics
rm -rf debian/source

log "create change log "
rm debian/changelog
debChangeLog


log "build deb package"
debuild -us -uc

popd $SCRIPTPATH/deb
