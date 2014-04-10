#!/bin/bash
#SCRIPT=$(readlink -f "$0")
#SCRIPTPATH=$(dirname "$SCRIPT")

pushd ..

pwd
echo "sbt package "
sbt package

echo "tar sbt package output"
mkdir -p usr/lib/intelanalytics/

cp -R target/scala-2.10/* usr/lib/intelanalytics/

tar -pczf source.tar.gz usr

popd 

tar -pczf source.tar.gz ../usr

echo "done packaging sbt" 
