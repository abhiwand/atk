#!/bin/bash

TITAN_URL=$1
TITAN_HOME=$2
TITAN_VERION=$3
MAVEN_URL=$4
MAVEN_HOME=$5
MAVEN_VERSION=$6
USE_INTERNAL=$7
ES_URL=$8
ES_HOME=$9
ES_VERSION=${10}
LAB_MACHINE=${11}
CFG_DIR=`pwd`/../cfg

HAS_MAVEN=`which mvn | wc -l`
if [ $HAS_MAVEN = 0 ]; then
    echo "install titan prerequisites - maven"
		source install_maven.sh $MAVEN_HOME $MAVEN_URL $MAVEN_VERSION $CFG_DIR
fi

HAS_ES=`which elasticsearch | wc -l`
if [ $HAS_ES = 0 ]; then
    echo "install titan prerequisites - elasticsearch"
    cd ~/Downloads
    wget $ES_URL/elasticsearch-$ES_VERSION.tar.gz
    tar xvfz elasticsearch-$ES_VERSION.tar.gz -C $ES_HOME
    sudo tee -a /etc/profile.d/gaousr.sh> /dev/null <<EOF
export PATH=$PATH:$ES_HOME/elasticsearch-$ES_VERSION/bin
EOF
    source /etc/profile
#    export PATH=$PATH:$ES_HOME/elasticsearch-$ES_VERSION/bin
fi


if [ -z `which git` ]; then
    echo "install titan prerequisites - git"
    sudo yum install -y git
fi

echo "checkout titan"
cd $TITAN_HOME
git clone $TITAN_URL
cd titan

if [ $USE_INTERNAL = "yes" ]; then
    git checkout tribeca
else
    git checkout $TITAN_VERSION
fi

echo "build titan"
mvn clean install -DskipTests
