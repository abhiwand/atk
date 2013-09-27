. ./versions.sh

rm -rf SOURCES/*
rm -rf git

mkdir -p git/SOURCES
pushd git

git clone https://github.com/thinkaurelius/titan.git
(cd titan && git checkout $TITAN_VERSION)
mv titan titan-$TITAN_VERSION
tar czf titan-$TITAN_VERSION.tar.gz titan-$TITAN_VERSION
mv titan-$TITAN_VERSION.tar.gz SOURCES

git clone https://github.com/thinkaurelius/faunus.git
(cd faunus && git checkout $FAUNUS_VERSION)
mv faunus faunus-$FAUNUS_VERSION
tar czf faunus-$FAUNUS_VERSION.tar.gz faunus-$FAUNUS_VERSION
mv faunus-$FAUNUS_VERSION.tar.gz SOURCES

git clone https://github.com/apache/giraph.git
(cd giraph && git checkout $GIRAPH_VERSION)
mv giraph giraph-$GIRAPH_VERSION 
tar czf giraph-$GIRAPH_VERSION.tar.gz giraph-$GIRAPH_VERSION
mv giraph-$GIRAPH_VERSION.tar.gz SOURCES

mv SOURCES/* ../SOURCES
popd
