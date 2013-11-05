#!/bin/bash

FAUNUS_URL=$1
FAUNUS_HOME=$2
FAUNUS_VERION=$3
MAVEN_URL=$4
MAVEN_HOME=$5
MAVEN_VERSION=$6
USE_INTERNAL=$7
LAB_MACHINE=$8
CFG_DIR=`pwd`/../cfg

HAS_MAVEN=`which mvn | wc -l`
if [ $HAS_MAVEN = 0 ]; then
	 echo "install giraph prerequisites - maven"
	 source install_maven.sh $MAVEN_HOME $MAVEN_URL $MAVEN_VERSION $CFG_DIR
fi

if [ -z `which git` ]; then
    echo "install faunus prerequisites - git"
    sudo yum install -y git
fi



echo "checkout faunus"
cd $FAUNUS_HOME
git clone $FAUNUS_URL
cd faunus
if [ $USE_INTERNAL = "yes" ]; then
    git checkout tribeca
else
    git checkout $FAUNUS_VERSION
fi

echo "build faunus"
mvn clean install -DskipTests
