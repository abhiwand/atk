#!/bin/bash
source common.sh
log create parcel

PACKAGE_NAME=INTELANALYTICS
parcelDir=$PACKAGE_NAME-$VERSION
tarFile=$2
rm -rf $SCRIPTPATH/$parcelDir

pushd $SCRIPTPATH


mkdir -p $parcelDir/tmp

cp -Rv parcel/* $parcelDir

sed -i "s/VERSION/${VERSION}/g" $parcelDir/meta/parcel.json

tar -xvf $2 -C $parcelDir/

for pythonPackage in `ls python/`
do
    echo $pythonPackage
    tar -xvf python/${pythonPackage} -C $parcelDir/
done

mv $parcelDir/usr/lib/intelanalytics/rest-client/python $parcelDir/usr/lib/python2.7/site-packages/intelanalytics

tar zcvf $parcelDir-el6.parcel $parcelDir/ --owner=root --group=root

mv $parcelDir-el6.parcel /home/rodorad/Documents/parcels/parcels/
python /home/rodorad/Documents/parcels/cm_ext/make_manifest/make_manifest.py /home/rodorad/Documents/parcels/parcels/
popd

#[name]-[version]-[distro suffix].parcel


#csd naming
#<name>-<csd-version>-<extra>.jar

