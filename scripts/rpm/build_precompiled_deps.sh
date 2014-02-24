. ./versions.sh


function abspath {
    if [[ -d "$1" ]]
    then
        pushd "$1" >/dev/null
        pwd
        popd >/dev/null
    elif [[ -e $1 ]]
    then
        pushd $(dirname "$1") >/dev/null
        echo "$(pwd)/$(basename "$1")"
        popd >/dev/null
    else
        echo "$1" does not exist! >&2
        return 127
    fi
}

package=intelanalytics-python-deps-precompiled

source_folder=SOURCES/$package-$TRIBECA_VERSION

#rm -rf $source_folder
rm -f $source_folder.tar.gz

mkdir -p $source_folder

tribeca_ia=$(abspath `dirname $0`/../../IntelAnalytics)

(cd /usr/lib/IntelAnalytics && sudo chmod 777 . && sudo rm -rf ./virtpy && $tribeca_ia/install_pyenv.sh)

cp -R /usr/lib/IntelAnalytics/virtpy SOURCES/$package-$TRIBECA_VERSION

(cd SOURCES/$package-$TRIBECA_VERSION && tar -czf template_overrides.tar.gz -C $tribeca_ia/ipython/TemplateOverrides .)

(cd SOURCES && tar czf $package-$TRIBECA_VERSION.tar.gz $package-$TRIBECA_VERSION)

rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/$package.spec
