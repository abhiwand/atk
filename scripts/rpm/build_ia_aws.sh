. ./versions.sh

package=intelanalytics-aws

source_folder=SOURCES/$package-$TRIBECA_VERSION

rm -rf $source_folder
rm -f $source_folder.tar.gz

mkdir -p $source_folder

src=$(abspath `dirname $0`/../../SaaS/aws/deploy)

shopt -s extglob

cp SPECS/$package.spec $package-$TRIBECA_VERSION
cp -R $src $source_folder

(cd SOURCES && tar cfz $package-$TRIBECA_VERSION.tar.gz $package-$TRIBECA_VERSION)

rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" --define "VERSION $TRIBECA_VERSION" -bb SPECS/$package.spec
