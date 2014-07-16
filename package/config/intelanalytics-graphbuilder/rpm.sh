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
REQUIRES="java >= 1.7, intelanalytics-spark-deps >= 0.8-${BUILD_NUMBER}, jq, perl-URI"

POST="
 #sim link to python sites packages
 if [ -f /usr/lib/intelanalytics/graphbuilder/ext/graphbuilder-3.jar ]; then
   rm /usr/lib/intelanalytics/graphbuilder/ext/graphbuilder-3.jar
 fi

 ln -s /usr/lib/intelanalytics/graphbuilder/graphbuilder-3.jar  /usr/lib/intelanalytics/graphbuilder/ext/graphbuilder-3.jar
"

#delete the sym link only if we are uninstalling not updating
POSTUN="

"

FILES="
/usr/lib/intelanalytics/graphbuilder/bin
/usr/lib/intelanalytics/graphbuilder/conf
/usr/lib/intelanalytics/graphbuilder/ext
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

