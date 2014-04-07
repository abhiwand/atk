#!/bin/bash
packageName=$1
tarFile=$2
version=$3
#tar -xvf $tarFile -C config/
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
echo $SCRIPTPATH

cp $tarFile $SCRIPTPATH/$package_$version.tar.gz
tar -xvf $tarFile -C $SCRIPTPATH/deb/debian

