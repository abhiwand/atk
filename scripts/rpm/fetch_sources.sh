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

#trib=tribeca-$TRIBECA_VERSION
#src=../../..
#rm -rf $trib 

#mkdir $trib
#cp -R $src/tribeca/target/*.jar $trib
#cp -R $src/tribeca/bin $trib
#cp -R $src/tribeca/dist/* $trib
#tar czvf $trib.tar.gz $trib

project_name=IntelAnalytics
source_folder=$project_name-$TRIBECA_VERSION
src=../../../$project_name
rm -rf $source_folder

mkdir $source_folder
cp -R $src/target/*.jar $source_folder
cp -R $src/bin $source_folder
cp -R $src/dist/* $source_folder
tar czvf $source_folder.tar.gz $source_folder
popd
