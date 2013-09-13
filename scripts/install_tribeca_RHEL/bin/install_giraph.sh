#!/bin/bash

GIRAPH_URL=$1
GIRAPH_HOME=$2
GIRAPH_VERSION=$3
MAVEN_URL=$4
MAVEN_HOME=$5
MAVEN_VERSION=$6
USE_INTERNAL=$7
LAB_MACHINE=$8
MAVEN_REPO_DIR=$9
CFG_DIR=`pwd`/../cfg

echo "GIRAPH_URL is " $GIRAPH_URL "GIRAPH_HOME" is $GIRAPH_HOME "GIRAPH_VERSION" is $GIRAPH_VERSION


HAS_MAVEN=`which mvn | wc -l`
if [ $HAS_MAVEN = 0 ]; then
    echo "install giraph prerequisites - maven"
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
