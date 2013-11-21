. ./versions.sh

if [ ! -d SOURCES ]
  then
    mkdir SOURCES
fi

pushd SOURCES

trib=intelanalytics-$TRIBECA_VERSION
src=../../..
rm -rf $trib 

mkdir $trib
cp -R $src/IntelAnalytics/target/*.jar $trib
cp -R $src/IntelAnalytics/bin $trib
cp -R $src/IntelAnalytics/dist/* $trib
tar czvf $trib.tar.gz $trib

popd
