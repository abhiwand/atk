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
#SUMMARY="$packageName$version Build number: $BUILD_NUMBER. TimeStamp $TIMESTAMP"
DESCRIPTION="$SUMMARY 
start the server with 'service intelanalytics status'
config files are in /etc/intelanalytics/rest-server
log files live in /var/log/intelanalytics/rest-server"

REQUIRES=" java-1.7.0-openjdk, intelanalytics-python-rest-client >= ${version}-${BUILD_NUMBER}, intelanalytics-graphbuilder >= ${version}-${BUILD_NUMBER}, python-argparse, python-cm-api"

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
	su -c \"hadoop fs -chown \$restUser:\$restUser /user/\$restUser\" hdfs
	su -c \"hadoop fs -chmod 755 /user/\$restUser\" hdfs
fi
"

POST="
restUser=iauser
if [ \$1 -eq 2 ]; then
  echo start intelanalytics
  service intelanalytics restart
fi

hadoop fs -ls /user/iauser/datasets 2>/dev/null
if [ \$? -eq 1 ]; then
	echo move sample data scripts and data sets
	cp -R /usr/lib/intelanalytics/rest-server/examples /home/\$restUser
	chown -R iauser:iauser /home/\$restUser/examples
	su -c \"hadoop fs -put ~/examples/datasets \" iauser
fi
 
"

PREUN="
 checkStatus=\$(service intelanalytics status | grep start/running)
 if  [ \$1 -eq 0 ] && [ \"\$checkStatus\" != \"\" ]; then
    echo stopping intelanalytics
    service intelanalytics stop
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

