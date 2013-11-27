. ./versions.sh

if [ ! -d SOURCES ]
  then
    mkdir SOURCES
fi

pushd SOURCES

project_name=intelanalytics
source_folder=$project_name-$TRIBECA_VERSION
python_folder=python-$source_folder
src=../../../IntelAnalytics
rm -rf $source_folder

mkdir $source_folder
cp -R $src/target/*.jar $source_folder
cp -R $src/bin $source_folder
rm -rf $source_folder/bin/*python*
cp -R $src/dist/* $source_folder
cp -R $src/conf $source_folder
rm -rf $source_folder/conf/*python*
mkdir $python_folder
cp -R $src/intel_analytics $python_folder

pushd $python_folder
tar czvf intel_analytics.tar.gz intel_analytics
rm -rf intel_analytics
popd

cp $source_folder/install_pyenv.sh $python_folder/
mkdir $python_folder/bin
cp -R $src/bin/IntelAnalytics-ipython $python_folder/bin
mkdir $python_folder/conf
cp -R $src/conf/ipython_profile $python_folder/conf
tar czvf $source_folder.tar.gz $source_folder
tar czvf $python_folder.tar.gz $python_folder
popd

