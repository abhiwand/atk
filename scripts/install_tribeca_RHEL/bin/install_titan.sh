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
MAVEN_REPO_DIR=${12}
CFG_DIR=`pwd`/../cfg

HAS_MAVEN=`which mvn | wc -l`
if [ $HAS_MAVEN = 0 ]; then
    echo "install titan prerequisites - maven"
    cd ~/Downloads
    wget $MAVEN_URL/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz
    tar xvfz apache-maven-$MAVEN_VERSION-bin.tar.gz -C $MAVEN_HOME
    if [ ! -d $MAVEN_REPO_DIR ]; then
          mkdir $MAVEN_REPO_DIR
    fi
    sudo chmod -R 775 $MAVEN_REPO_DIR 
    SETTING="<localRepository>$MAVEN_REPO_DIR</localRepository>"
    sed -i '50d' $CFG_DIR/settings.xml
    sed -i "50i$SETTING" $CFG_DIR/settings.xml
    if [ $LAB_MACHINE = "yes" ]; then
       cp $CFG_DIR/settings.xml $MAVEN_HOME/apache-maven-$MAVEN_VERSION/conf/
    fi
    sudo tee -a /etc/profile.d/gaousr.sh> /dev/null <<EOF
export PATH=$PATH:$MAVEN_HOME/apache-maven-$MAVEN_VERSION/bin
EOF
    source /etc/profile
#    export PATH=$PATH:$MAVEN_HOME/apache-maven-$MAVEN_VERSION/bin
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
