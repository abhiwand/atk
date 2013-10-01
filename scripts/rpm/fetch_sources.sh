. ./versions.sh

pushd SOURCES
if [ ! -f hbase-$HBASE_VERSION.tar.gz ] 
  then
   wget https://github.com/apache/hbase/archive/$HBASE_VERSION.tar.gz -O hbase-$HBASE_VERSION.tar.gz
fi 

if [ ! -f titan-$TITAN_VERSION.tar.gz ]
  then
    wget https://github.com/thinkaurelius/titan/archive/$TITAN_VERSION.tar.gz -O titan-$TITAN_VERSION.tar.gz
fi

if [ ! -f faunus-$FAUNUS_VERSION.tar.gz ]
  then 
    wget https://github.com/thinkaurelius/faunus/archive/$FAUNUS_VERSION.tar.gz -O faunus-$FAUNUS_VERSION.tar.gz
fi

if [ ! -f giraph-$GIRAPH_VERSION.tar.gz ]
  then
    wget https://github.com/apache/giraph/archive/$GIRAPH_VERSION.tar.gz -O giraph-$GIRAPH_VERSION.tar.gz
fi

popd
