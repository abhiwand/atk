. ./versions.sh

if [ ! -d SOURCES ]
  then
    mkdir SOURCES
fi

pushd SOURCES
if [ ! -f hbase-$HBASE_VERSION.tar.gz ]
  then
   wget https://github.com/apache/hbase/archive/$HBASE_VERSION.tar.gz -O hbase-$HBASE_VERSION.tar.gz
fi

if [ ! -f titan-$TITAN_VERSION.tar.gz ]
  then
    wget https://github.com/thinkaurelius/titan/archive/$TITAN_VERSION.tar.gz -O titan-$TITAN_VERSION.tar.gz
fi

project_name=intelanalytics
source_folder=$project_name-$TRIBECA_VERSION
python_folder=python-$source_folder
src=../../../IntelAnalytics
rm -rf $source_folder

mkdir $source_folder
cp -R $src/target/*.jar $source_folder
cp -R $src/bin $source_folder
cp -R $src/dist/* $source_folder

cp -R $src/intel_analytics $python_folder

pushd $python_folder
tar czvf intel_analytics.tar.gz intel_analytics
rm -rf intel_analytics
popd

mkdir $devel_folder
cp $source_folder/install_pyenv.sh $python_folder/

tar czvf $source_folder.tar.gz $source_folder
tar czvf $python_folder.tar.gz $python_folder
popd

