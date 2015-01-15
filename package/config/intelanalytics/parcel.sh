#!/bin/bash
source common.sh
log create parcel

PACKAGE_NAME=INTELANALYTICS
parcelDir=$PACKAGE_NAME-$VERSION-$BUILD_NUMBER.cdh5.2.0
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

mkdir -p $parcelDir/usr/lib/python2.7/site-packages/intelanalytics
cp -Rv $parcelDir/usr/lib/intelanalytics/rest-client/python/* $parcelDir/usr/lib/python2.7/site-packages/intelanalytics
rm -rf $parcelDir/usr/lib/intelanalytics/rest-client/python

tar zcvf $parcelDir-el6.parcel $parcelDir/ --owner=root --group=root

cp $parcelDir-el6.parcel /home/rodorad/Documents/parcels/parcels/
python /home/rodorad/Documents/parcels/cm_ext/make_manifest/make_manifest.py /home/rodorad/Documents/parcels/parcels/
popd

rm -rf $parcelDir
#[name]-[version]-[distro suffix].parcel


#csd naming
#<name>-<csd-version>-<extra>.jar

