. ./versions.sh

package=intelanalytics-python

source_folder=SOURCES/$package-$TRIBECA_VERSION

rm -rf $source_folder
rm -f $source_folder.tar.gz

mkdir -p $source_folder

src=$(abspath `dirname $0`/../../IntelAnalytics)

shopt -s extglob

cp -R $src/intel_analytics $source_folder
mkdir -p $source_folder/bin
mkdir -p $source_folder/conf

cp $src/bin/*python* $source_folder/bin
cp -R $src/conf/*python* $source_folder/conf
cp $src/conf/intel_analytics.properties $source_folder/conf
cp $src/conf/pig_log4j.properties $source_folder/conf
cp $src/conf/ipython_notebook_config.py $source_folder/conf
cp -R $src/notebooks $source_folder/

(cd SOURCES && tar czf $package-$TRIBECA_VERSION.tar.gz $package-$TRIBECA_VERSION)

rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" --define "IAUSER $IAUSER"  -bb SPECS/$package.spec
