#!/bin/bash
source common.sh

log "Build rpm package"

packageName=$1
tarFile=$2
TAR_FILE=$tarFile
version=$3

#deleteOldBuildDirs

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

log "copy and rename: $tarFile"
mkdir -p $SCRIPTPATH/rpm/SOURCES
cp $tarFile $SCRIPTPATH/rpm/SOURCES/${packageName}-${version}.tar.gz

LICENSE="Confidential"
SUMMARY="$packageName-$version Build number: $BUILD_NUMBER. TimeStamp $TIMESTAMP"
DESCRIPTION="$SUMMARY 
start the server with 'service intelanalytics-rest-server status' 
config files are in /etc/intelanalytics/rest-server
log files live in /var/log/intelanalytics/rest-server"

REQUIRES=" java >= 1.7, intelanalytics-python-rest-client >= 0.8-${BUILD_NUMBER}, intelanalytics-graphbuilder >= 0.8-${BUILD_NUMBER}"

PRE="
restUser=iauser
if [ \"\`cat /etc/passwd | grep \$restUser\`\" == \"\" ]; then
	echo create \$restUser
	useradd -G hadoop \$restUser	
fi

hadoop fs -ls /user/iauser 2>/dev/null
if [ \$? -eq 1 ]; then
	echo create \$restUser hdfs home directory
	su -c \"hadoop fs -mkdir /user/\$restUser\" hdfs
	su -c \"hadoop fs -chown iauser:iauser /user/\$restUser\" hdfs
	su -c \"hadoop fs -chmod 755 /user/\$restUser\" hdfs
fi
"

POST="

 if [ \$1 -eq 2 ]; then
    echo start intelanalytics-rest-server
    service intelanalytics-rest-server restart
 fi
 
"

PREUN="
 checkStatus=\$(service intelanalytics-rest-server status | grep start/running)
 if  [ \$1 -eq 0 ] && [ \"\$checkStatus\" != \"\" ]; then
    echo stopping intelanalytics-rest-server
    service intelanalytics-rest-server stop
 fi
"

FILES="
/etc/intelanalytics/rest-server
/usr/lib/intelanalytics/rest-server
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
pwd
rpmbuild --define "_topdir $topDir"  --define "BUILD_NUMBER $BUILD_NUMBER" --define "VERSION $VERSION" -bb SPECS/$packageName.spec

cleanRpm

popd 

