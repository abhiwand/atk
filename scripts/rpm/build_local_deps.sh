. ./versions.sh

package=intelanalytics-python-deps-localbuild

source_folder=SOURCES/$package-$TRIBECA_VERSION

rm -rf $source_folder
rm -f $source_folder.tar.gz

mkdir -p $source_folder

src=$(abspath `dirname $0`/../../IntelAnalytics)
cp $src/install_pyenv.sh $source_folder
tar -cvzf $source_folder/template_overrides.tar.gz -C $src/ipython/TemplateOverrides .

(cd SOURCES && tar czf $package-$TRIBECA_VERSION.tar.gz $package-$TRIBECA_VERSION)

rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/$package.spec
