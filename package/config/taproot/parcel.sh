#!/bin/bash
source common.sh
log create parcel

PACKAGE_NAME=INTELANALYTICS
parcelDir=$PACKAGE_NAME-$VERSION-$BUILD_NUMBER.cdh5.3.0
tarFile=$2

rm -rf $SCRIPTPATH/$parcelDir


pushd $SCRIPTPATH

if [ -d /root/python ]; then
    cp -Rv /root/python/* python/
fi

mkdir -p $parcelDir/tmp
mkdir -p $parcelDir/log

cp -Rv parcel/* $parcelDir

sed -i "s/VERSION/${VERSION}/g" $parcelDir/meta/parcel.json
sed -i "s/BUILD/${BUILD_NUMBER}/g" $parcelDir/meta/parcel.json

tar -xvf $2 -C $parcelDir/

for pythonPackage in `ls python/`
do
    echo $pythonPackage
    tar -xvf python/${pythonPackage} -C $parcelDir/
done

mkdir -p $parcelDir/usr/lib/python2.7/site-packages/taprootanalytics
cp -Rv $parcelDir/usr/lib/taprootanalytics/rest-client/python/* $parcelDir/usr/lib/python2.7/site-packages/taprootanalytics
rm -rf $parcelDir/usr/lib/taprootanalytics/rest-client/python

tar zcvf $parcelDir-el6.parcel $parcelDir/ --owner=root --group=root

popd

rm -rf $parcelDir
#[name]-[version]-[distro suffix].parcel


#csd naming
#<name>-<csd-version>-<extra>.jar

