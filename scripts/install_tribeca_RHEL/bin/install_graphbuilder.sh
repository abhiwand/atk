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
#    export  PATH=$PATH:$MAVEN_HOME/apache-maven-$MAVEN_VERSION/bin
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

