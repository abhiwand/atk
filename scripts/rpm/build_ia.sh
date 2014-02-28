. ./versions.sh

package=intelanalytics

source_folder=SOURCES/$package-$TRIBECA_VERSION

rm -rf $source_folder
rm -f $source_folder.tar.gz

mkdir -p $source_folder
mkdir $source_folder/target

src=$(abspath `dirname $0`/../../IntelAnalytics)

shopt -s extglob
cp -R $src/!(exclude|dist|build|src|notebooks|tests|*.ipynb|ipython|install_pyenv.sh|intel_analytics|checkstyle*|*pom.xml|test*|nose*|cover) $source_folder
cp $src/target/*.jar $src/target/*.pig $source_folder/target/
rm -f $source_folder/target/original-*.jar
rm -f $source_folder/target/*-tests.jar
rm -f $source_folder/original-*.jar
rm -rf $source_folder/bin/*python*
rm -rf $source_folder/conf/*python*
rm -rf $source_folder/conf/intel_analytics.properties

(cd SOURCES && tar czf $package-$TRIBECA_VERSION.tar.gz $package-$TRIBECA_VERSION)

rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/intelanalytics.spec
