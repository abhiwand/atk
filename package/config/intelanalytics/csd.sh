#!/bin/bash
source common.sh
log "Create csd"

PACKAGE_NAME=INTELANALYTICS

pkgFolder=$PACKAGE_NAME-$VERSION.$BUILD_NUMBER
pkgPath=$SCRIPTPATH/$PACKAGE_NAME.$VERSION-$BUILD_NUMBER

rm -rf $pkgPath

pushd $SCRIPTPATH
env

mkdir -p $pkgFolder
cp -Rv csd/* $pkgFolder/
sed -i "s/VERSION/${VERSION}/g" $pkgFolder/descriptor/service.sdl
sed -i "s/BUILD/${BUILD_NUMBER}/g" $pkgFolder/descriptor/service.sdl


pushd $pkgFolder
    jar -cvf $pkgFolder.jar *
popd

#cp $pkgFolder/$pkgFolder.jar /home/rodorad/Documents/parcels/csd
#scp $pkgFolder/$pkgFolder.jar wolverine:~
#scp $pkgFolder/$pkgFolder.jar tungsten:~
#ssh wolverine sudo cp /home/hadoop/$pkgFolder.jar /opt/cloudera/csd
#ssh tungsten sudo cp /home/hadoop/$pkgFolder.jar /opt/cloudera/csd
popd

rm -rf $pkgFolder