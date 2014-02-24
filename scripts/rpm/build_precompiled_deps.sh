. ./versions.sh

package=intelanalytics-python-deps-precompiled

source_folder=SOURCES/$package-$TRIBECA_VERSION

#rm -rf $source_folder
rm -f $source_folder.tar.gz

mkdir -p $source_folder

tribeca_ia=$(abspath `dirname $0`/../../IntelAnalytics)

#Writing to /usr/lib/IntelAnalytics/virtpy requires that /usr/lib/IntelAnalytics be writable by this user.
(cd /usr/lib/IntelAnalytics && rm -rf ./virtpy && $tribeca_ia/install_pyenv.sh)

cp -R /usr/lib/IntelAnalytics/virtpy SOURCES/$package-$TRIBECA_VERSION

(cd SOURCES/$package-$TRIBECA_VERSION && tar -czf template_overrides.tar.gz -C $tribeca_ia/ipython/TemplateOverrides .)

(cd SOURCES && tar czf $package-$TRIBECA_VERSION.tar.gz $package-$TRIBECA_VERSION)

rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/$package.spec
