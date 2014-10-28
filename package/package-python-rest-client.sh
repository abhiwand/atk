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

mkdir -p  tarballs/$package/usr/lib/intelanalytics/rest-client/python/doc


cp -v config/intelanalytics-python-rest-client/requirements.txt tarballs/$package/usr/lib/intelanalytics/rest-client/python/

#releaseNumber=$(echo $BRANCH | awk '/release_[0-9.]*$/{print substr($0, match($0,"[0-9.]*$"))}')
#if [ "$releaseNumber" != "" ]; then
#    mkdir -p tarballs/$package/usr/lib/intelanalytics/rest-client/python/rest
#    cp ../python/intelanalytics/rest/config.py tarballs/$package/usr/lib/intelanalytics/rest-client/python/rest/config.py
#   find ../python/intelanalytics -name *.py -type f -delete
#    ls -l ../python/intelanalytics/core
#    cp -Rv  ../python/intelanalytics/* tarballs/$package/usr/lib/intelanalytics/rest-client/python/
#    echo "remove py files"
#else
    cp -Rv  ../python/intelanalytics/* tarballs/$package/usr/lib/intelanalytics/rest-client/python/
#fi

cp -Rv  ../python/cmdgen.py tarballs/$package/usr/lib/intelanalytics/rest-client/

cp -Rv ../doc/build/html tarballs/$package/usr/lib/intelanalytics/rest-client/python/doc/html/

find ../python/intelanalytics/ -type f -name "*.pyc" -exec rm -f {} \;

popd

pushd $SCRIPTPATH/tarballs/$package

tar -pczf ../../$package-source.tar.gz .

popd
