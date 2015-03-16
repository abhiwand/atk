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
log "mkdir -p $SCRIPTPATH/rpm/SOURCES"
mkdir -p $SCRIPTPATH/rpm/SOURCES
log "cp $tarFile $SCRIPTPATH/rpm/SOURCES/${packageName}-${version}.tar.gz"
cp $tarFile $SCRIPTPATH/rpm/SOURCES/${packageName}-${version}.tar.gz

GROUP="Intel Analytics Server"
LICENSE="Confidential"
#SUMMARY="$packageName$version Build number: $BUILD_NUMBER. TimeStamp $TIMESTAMP"
DESCRIPTION="$SUMMARY \\n
start the server with 'service intelanalytics status' \\n
config files are in /etc/intelanalytics/rest-server \\n
log files live in /var/log/intelanalytics/rest-server \\n

Intel ATK Release Notes - 2014-10-28 \\n

The following changes have been made as part of the ATK 0.8.7 release.\\n

- TRIB-1517 - Added support for Outer Joins \\n
- TRIB-3190 - Tuned the HBase and Spark settings to improve performance \\n
- TRIB-3809 - Added experimental implemenation of GraphX PageRank \\n
- TRIB-2885 - Fixed shared jars \\n
- TRIB-3846 - Move to support Python 2.7 \\n
- TRIB-3771 - Fixed schema rollback issue on failed add_column \\n
- TRIB-3741 - Improved error messages on index out of range for rows \\n
- TRIB-3824 - Better handling of invalid column names \\n
- TRIB-3834 - Added documenation to not use unicode characters in column names \\n
- TRIB-3851 - Graphs should not be named with special charactes documentation \\n
- TRIB-3858 - Documentation updated to ALS and CGD on how to specify L and R vertices \\n

"

REQUIRES=" java-1.7.0-openjdk, intelanalytics-python-rest-client >= ${version}-${BUILD_NUMBER}, python-argparse, python-cm-api, postgresql-server"

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
deployJar=deploy.jar

jars=\"engine-spark.jar igiraph-titan.jar graphon.jar\"

for jar in $jars
do
if [ -d /usr/lib/intelanalytics/rest-server/lib/\$jar ]; then
   rm /usr/lib/intelanalytics/rest-server/lib/\$jar
 fi

 ln -s /usr/lib/intelanalytics/rest-server/lib/\$deployJar  /usr/lib/intelanalytics/rest-server/lib/\$jar
done

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


log "mkdir -p $SCRIPTPATH/rpm/SPECS"
mkdir -p $SCRIPTPATH/rpm/SPECS
log "rpmSpec > $SCRIPTPATH/rpm/SPECS/$packageName.spec"
env
rpmSpec > $SCRIPTPATH/rpm/SPECS/$packageName.spec

log "topdir "
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

