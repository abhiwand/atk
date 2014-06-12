#!/bin/bash
package="intelanalytics-spark-deps"
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

pwd

rm -rf tarballs/$package
rm $package-source.tar.gz

mkdir -p  tarballs/$package/usr/lib/intelanalytics/graphbuilder/lib

jarPath=$(find ..  -path ./package -prune -o -name ispark-deps.jar -print)

echo $jarPath
cp -v $jarPath tarballs/$package/usr/lib/intelanalytics/graphbuilder/lib/

pushd tarballs/$package

tar -pczf ../../$package-source.tar.gz .

popd

rm -rf tarballs
