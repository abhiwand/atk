#!/bin/bash
package="intelanalytics-graphbuilder"
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

mkdir -p  tarballs/$package/usr/lib/intelanalytics/graphbuilder

#cp -Rv ../ititan/bin tarballs/$package/usr/lib/intelanalytics/graphbuilder/bin/
#cp -Rv ../ititan/conf tarballs/$package/usr/lib/intelanalytics/graphbuilder/conf/
#cp -Rv ../ititan/ext tarballs/$package/usr/lib/intelanalytics/graphbuilder/ext/


jarPath=$(find ..  -path ./package -prune -o -name graphbuilder-3.jar -print)

echo $jarPath
cp -v $jarPath tarballs/$package/usr/lib/intelanalytics/graphbuilder/

cp -v config/intelanalytics-graphbuilder/set-cm-spark-classpath.sh   tarballs/$package/usr/lib/intelanalytics/graphbuilder/
chmod +x tarballs/$package/usr/lib/intelanalytics/graphbuilder/set-cm-spark-classpath.sh

pushd tarballs/$package

tar -pczf ../../$package-source.tar.gz .

popd

rm -rf tarballs
