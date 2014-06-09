#!/bin/bash
source common.sh

log "Build rpm package"

packageName=$1
tarFile=$2
TAR_FILE=$tarFile
version=$3


topDir="$SCRIPTPATH/rpm"
#exit 1
pushd $SCRIPTPATH/rpm

log "clean up build dirs"
rm -rf BUILD/*
rm -rf BUILDROOT/*


log $BUILD_NUMBER

echo "top dir $topDir"

rpmbuild --define "_topdir $topDir" --define "pythonVersion 2.7.5" --define "packageVersion $VERSION" --define "baseName pandas" --define "BUILD_NUMBER $BUILD_NUMBER" --define "VERSION $VERSION" -ba SPECS/$packageName.spec

popd $SCRIPTPATH/rpm 

