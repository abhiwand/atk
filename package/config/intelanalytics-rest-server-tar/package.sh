#!/bin/bash
package="intelanalytics-rest-server-tar"
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

#pushd $SCRIPTPATH

rm -rf ../bin/stage
rm -rf tarballs/$package
rm $package-source.tar.gz

mkdir -p  tarballs/$package/bin
mkdir -p  tarballs/$package/conf
mkdir -p  tarballs/$package/lib


cp -v ../conf/examples/application.conf.tpl tarballs/$package/conf
cp -v ../conf/examples/application.conf.single-system.tpl tarballs/$package/conf
cp -Rv config/intelanalytics-rest-server/assets/etc/intelanalytics/rest-server/* tarballs/$package/conf
cp -v  config/intelanalytics-rest-server-tar/api-server.sh tarballs/$package/bin/


jars="engine-spark.jar api-server.jar engine.jar interfaces.jar igiraph-titan.jar graphon.jar"

#popd

pushd $gitRoot
for jar in $jars
do
	jarPath=$(find .  -path ./package -prune -o -name $jar -print )
	echo $jarPath
	cp -v $jarPath package/tarballs/$package/lib/

done

jarPath=$(find .  -path ./package -prune -o -name launcher.jar -print)

echo $jarPath
#enable this to copy the regular launcher.jar to the correct place
cp -v $jarPath package/tarballs/$package/launcher.jar


popd


pushd tarballs/$package
    tar -pczf ../../atk.tar.gz .
popd


rm -rf tarballs