. versions.sh

if [ ! -d SOURCES ]
  then
    mkdir SOURCES
fi

rm -rf SOURCES/tribeca-$TRIBECA_VERSION

mkdir SOURCES/tribeca-$TRIBECA_VERSION

cd SOURCES/tribeca-$TRIBECA_VERSION

cp -R ../../../../tribeca/* .

cd ..

tar czf tribeca-$TRIBECA_VERSION.tar.gz tribeca-$TRIBECA_VERSION

cd ..

	
