#!/bin/bash
package="intelanalytics-rest-server"
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

rm -rf ../bin/stage
rm -rf tarballs/$package
rm $package-source.tar.gz



mkdir -p  tarballs/$package/etc/intelanalytics/rest-server

mkdir -p  tarballs/$package/usr/lib/intelanalytics/rest-server/lib

cp -Rv ../api-server/src/main/resources/* tarballs/$package/etc/intelanalytics/rest-server
cp -Rv ../engine/src/main/resources/* tarballs/$package/etc/intelanalytics/rest-server
cp -Rv config/intelanalytics-rest-server/assets/* tarballs/$package/

jars="engine-spark.jar api-server.jar engine.jar interfaces.jar "

popd

pushd $gitRoot
for jar in $jars
do 
	jarPath=$(find .  -path ./package -prune -o -name $jar -print )
	echo $jarPath
	cp -v $jarPath $SCRIPTPATH/tarballs/$package/usr/lib/intelanalytics/rest-server/lib/
done
	jarPath=$(find .  -path ./package -prune -o -name launcher.jar -print)
	echo $jarPath
	cp -v $jarPath $SCRIPTPATH/tarballs/$package/usr/lib/intelanalytics/rest-server/launcher.jar
	cp -v $jarPath $SCRIPTPATH/tarballs/$package/usr/lib/intelanalytics/rest-server/lib/

popd

pushd $SCRIPTPATH/tarballs/$package

tar -pczf ../../$package-source.tar.gz .

popd

rm -rf tarballs
