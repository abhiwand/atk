#!/bin/bash

GRAPHBUILDER_URL=$1
GRAPHBUILDER_HOME=$2
GRAPHBUILDER_VERION=$3
MAVEN_URL=$4
MAVEN_HOME=$5
MAVEN_VERSION=$6
LAB_MACHINE=$7
MAVEN_REPO_DIR=$8
CFG_DIR=`pwd`/../cfg

echo "install graphbuilder prerequisites - maven"
HAS_MAVEN=`which mvn | wc -l`
if [ $HAS_MAVEN = 0 ]; then
	 echo "install giraph prerequisites - maven"
   source install_maven.sh $MAVEN_HOME $MAVEN_URL $MAVEN_VERSION $CFG_DIR
fi

if [ -z `which git` ]; then
    echo "install graphbuilder prerequisites - git"
    sudo yum install -y git
fi

echo "checkout graphbuilder"
cd $GRAPHBUILDER_HOME
git clone $GRAPHBUILDER_URL
cd graphbuilder
###use latest master version
#checkout $GRAPHBUILDER_VERION

echo "build graphbuilder"
mvn clean
mvn package

