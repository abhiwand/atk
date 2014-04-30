#!/bin/bash
source common.sh

log "Build rpm package"

packageName=$1
tarFile=$2
TAR_FILE=$tarFile
version=$3

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

log "copy and rename: $tarFile"
cp $tarFile $SCRIPTPATH/rpm/SOURCES/${packageName}-${version}.tar.gz

SUMMARY="$packageName-$version Build number: $BUILD_NUMBER. TimeStamp $TIMESTAMP"
DESCRIPTION=$SUMMARY

rpmSpec > $SCRIPTPATH/rpm/SPECS/$packageName.spec

topDir="$SCRIPTPATH/rpm"
#exit 1
pushd $SCRIPTPATH/rpm

log "clean up build dirs"
rm -rf BUILD/*
rm -rf BUILDROOT/*


log $BUILD_NUMBER
rpmbuild --define "_topdir $topDir"  --define "BUILD_NUMBER $BUILD_NUMBER" --define "VERSION $VERSION" -bb SPECS/$packageName.spec

popd $SCRIPTPATH/rpm 

