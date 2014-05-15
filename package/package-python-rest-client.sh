#!/bin/bash
package="intelanalytics-python-rest-client"
workDir=$(pwd)
baseDir=${workDir##*/}
gitRoot="."
if [ "$baseDir" == "package" ]; then
	source common.sh
	gitRoot=".."
	else
	source package/common.sh
	gitRoot="."
fi

echo "$SCRIPTPATH" 

pushd $SCRIPTPATH

rm -rf tarballs/$package
rm $package-source.tar.gz

#mkdir -p  tarballs/$package/etc/intelanalytics/rest-server

mkdir -p  tarballs/$package/usr/lib/intelanalytics/rest-client/python/

cp -v config/intelanalytics-python-rest-client/requirements.txt tarballs/$package/usr/lib/intelanalytics/rest-client/python/

cp -Rv  ../python/intelanalytics/* tarballs/$package/usr/lib/intelanalytics/rest-client/python/

popd

pushd $SCRIPTPATH/tarballs/$package

tar -pczf ../../$package-source.tar.gz .

popd
