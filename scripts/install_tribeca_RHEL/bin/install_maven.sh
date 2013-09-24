#!/bin/bash

MAVEN_HOME=$1
MAVEN_URL=$2
MAVEN_VERSION=$3
CFG_DIR=$4

pushd

cd ~/Downloads
wget $MAVEN_URL/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz
tar xvfz apache-maven-$MAVEN_VERSION-bin.tar.gz -C $MAVEN_HOME
if [ ! -d $MAVEN_REPO_DIR ]; then
   mkdir $MAVEN_REPO_DIR
fi
 chmod -R 775 $MAVEN_REPO_DIR
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

popd
