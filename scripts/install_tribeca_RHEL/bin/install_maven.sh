#!/bin/bash

MAVEN_HOME=$1
MAVEN_URL=$2
MAVEN_VERSION=$3
CFG_DIR=$4

pushd ~/Downloads
wget $MAVEN_URL/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz
tar xvfz apache-maven-$MAVEN_VERSION-bin.tar.gz -C $MAVEN_HOME
 
cp $CFG_DIR/settings.xml $MAVEN_HOME/apache-maven-$MAVEN_VERSION/conf/
 
 sudo tee -a /etc/profile.d/gaousr.sh> /dev/null <<EOF
 export PATH=$PATH:$MAVEN_HOME/apache-maven-$MAVEN_VERSION/bin
EOF
 source /etc/profile

popd
