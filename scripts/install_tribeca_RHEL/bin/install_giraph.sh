#!/bin/bash

GIRAPH_URL=$1
GIRAPH_HOME=$2
GIRAPH_VERSION=$3
MAVEN_URL=$4
MAVEN_HOME=$5
MAVEN_VERSION=$6
USE_INTERNAL=$7
LAB_MACHINE=$8
CFG_DIR=`pwd`/../cfg

echo "GIRAPH_URL is " $GIRAPH_URL "GIRAPH_HOME" is $GIRAPH_HOME "GIRAPH_VERSION" is $GIRAPH_VERSION


HAS_MAVEN=`which mvn | wc -l`
if [ $HAS_MAVEN = 0 ]; then
    echo "install giraph prerequisites - maven"
		source install_maven.sh $MAVEN_HOME $MAVEN_URL $MAVEN_VERSION $CFG_DIR 
fi

echo "checkout giraph"
echo $GIRAPH_HOME
cd $GIRAPH_HOME
git clone $GIRAPH_URL
cd giraph

if [ $USE_INTERNAL = "yes" ]; then
    git checkout tribeca
else
    git checkout $GIRAPH_VERSION
fi

echo "build giraph"
mvn clean install -DskipTests
