#!/bin/bash
source common.sh
log "Build deb package"

packageName=$1
tarFile=$2
version=$3

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

log "copy tar.gz and rename for packaing"
expandTarDeb

SOURCE=$packageName
SUMMARY="zombies "
DESCRIPTION=$SUMMARY
SUBJECT=$DESCRIPTION

log "create control file"
debControl > $SCRIPTPATH/deb/debian/control

log "create compat file"
debCompat >  $SCRIPTPATH/deb/debian/compat

log "create install file"
debInstall > $SCRIPTPATH/deb/debian/$packageName.install

log "create copyright file"
debCopyright >  $SCRIPTPATH/deb/debian/copyright
pushd $SCRIPTPATH/deb

rm -rf debian/intelanalytics
rm -rf debian/source

log "create change log "
rm debian/changelog
debChangeLog

log "build deb package"
debuild -us -uc

popd $SCRIPTPATH/deb
