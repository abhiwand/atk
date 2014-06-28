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

jars="ispark-deps.jar igiraph-titan.jar"
for jar in $jars
do
	jarPath=$(find ..  -path ./package -prune -o -name $jar -print)
	echo $jarPath
    cp -v $jarPath tarballs/$package/usr/lib/intelanalytics/graphbuilder/lib/
done

pushd tarballs/$package

tar -pczf ../../$package-source.tar.gz .

popd

rm -rf tarballs
