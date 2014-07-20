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

mkdir confunpack
jarPath=$(find ..  -path ./package -prune -o -name conf.jar -print )
echo $jarPath
unzip $jarPath -d confunpack

cp -v confunpack/reference.conf .

rm -rf confunpack

mkdir launcher
jarPath=$(find ..  -path ./package -prune -o -name launcher.jar -print )
echo "launcher path " launcher/$jarPath
cp $jarPath launcher/

pushd launcher

unzip launcher.jar
rm launcher.jar
popd 

cp reference.conf launcher/

pushd launcher
jar -cvf launcher.jar *
popd



mkdir -p  tarballs/$package/etc/intelanalytics/rest-server

mkdir -p  tarballs/$package/usr/lib/intelanalytics/rest-server/lib

ls -l launcher/launcher.jar
echo copy new jar
pwd



cp -v launcher/launcher.jar $SCRIPTPATH/tarballs/$package/usr/lib/intelanalytics/rest-server/
rm -rf launcher
cp -v ../conf/examples/application.conf.tpl tarballs/$package/etc/intelanalytics/rest-server
#cp -Rv ../api-server/src/main/resources/* tarballs/$package/etc/intelanalytics/rest-server
#cp -Rv ../engine/src/main/resources/* tarballs/$package/etc/intelanalytics/rest-server
cp -Rv config/intelanalytics-rest-server/assets/* tarballs/$package/ 
#log "add build number to application conf"
#sed -i "s/^intel.analytics.api.buildId.*/intel.analytics.api.buildId = \"$BUILD_NUMBER\"/g" tarballs/$package/etc/intelanalytics/rest-server/application.conf


jars="engine-spark.jar api-server.jar engine.jar interfaces.jar igiraph-titan.jar graphon.jar"

popd

pushd $gitRoot
for jar in $jars
do 
	jarPath=$(find .  -path ./package -prune -o -name $jar -print )
	echo $jarPath
	cp -v $jarPath $SCRIPTPATH/tarballs/$package/usr/lib/intelanalytics/rest-server/lib/

done
#	jarPath=$(find .  -path ./package -prune -o -name launcher.jar -print)

#	echo $jarPath
#	cp -v $jarPath $SCRIPTPATH/tarballs/$package/usr/lib/intelanalytics/rest-server/launcher.jar
#	cp -v $jarPath $SCRIPTPATH/tarballs/$package/usr/lib/intelanalytics/rest-server/lib/

popd

pushd $SCRIPTPATH/tarballs/$package

tar -pczf ../../$package-source.tar.gz .

popd

#rm -rf tarballs
