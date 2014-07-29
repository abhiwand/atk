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

LICENSE="Confidential"
SUMMARY="$packageName-$version Build number: $BUILD_NUMBER. TimeStamp $TIMESTAMP"
DESCRIPTION=$SUMMARY 
REQUIRES="python, python-ordereddict, python-pip, numpy >= 1.8.1, python-pandas >= 0.13.1, python-bottle >= 0.12, python-requests >= 2.2.1"

POST="
 #sim link to python sites packages
 if [ -d /usr/lib/python2.6/site-packages/intelanalytics ]; then
   rm /usr/lib/python2.6/site-packages/intelanalytics
 fi

 ln -s /usr/lib/intelanalytics/rest-client/python  /usr/lib/python2.6/site-packages/intelanalytics
"

#delete the sym link only if we are uninstalling not updating
POSTUN="
 if  [ \$1 -eq 0 ]; then
    rm /usr/lib/python2.6/site-packages/intelanalytics
 fi
"

FILES="
/usr/lib/intelanalytics/rest-client
%config /usr/lib/intelanalytics/rest-client/python/rest/config.py
"

mkdir -p $SCRIPTPATH/rpm/SPECS
rpmSpec > $SCRIPTPATH/rpm/SPECS/$packageName.spec

topDir="$SCRIPTPATH/rpm"
#exit 1
pushd $SCRIPTPATH/rpm

log "clean up build dirs"
rm -rf BUILD/*
rm -rf BUILDROOT/*


log $BUILD_NUMBER
rpmbuild --define "_topdir $topDir"  --define "BUILD_NUMBER $BUILD_NUMBER" --define "VERSION $VERSION" -bb SPECS/$packageName.spec

cleanRpm
popd

