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

trib=tribeca-$TRIBECA_VERSION
src=../../..
rm -rf $trib 

mkdir $trib
cp -R $src/tribeca/target/*.jar $trib
cp -R $src/tribeca/dist/* $trib
tar czvf $trib.tar.gz $trib

popd
