#!/bin/bash

MAHOUT_URL=$1
MAHOUT_HOME=$2
MAHOUT_VERSION=$3
MAVEN_URL=$4
MAVEN_HOME=$5
MAVEN_VERSION=$6
USE_INTERNAL=$7
LAB_MACHINE=$8
MAVEN_REPO_DIR=$9
CFG_DIR=`pwd`/../cfg


echo "install mahout prerequisites - maven"
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

echo "checkout mahout"
cd $MAHOUT_HOME

if [ $USE_INTERNAL = "yes" ]; then
    if [ -z `which git` ]; then
        echo "install mahout prerequisites - git"
        sudo yum install -y git
    fi
    git clone $MAHOUT_URL
    cd mahout
    git checkout tribeca
else
    HAS_SVN=`which svn | wc -l`
    if [ $HAS_SVN -ne 1 ]; then
       echo "install mahout prerequisites - svn"
       sudo yum install -y subversion
    fi

    if [ $LAB_MACHINE = "yes" ]; then
         echo "set up proxy for svn"
         sudo cp $CFG_DIR/servers /etc/subversion/
    fi

    svn co $MAHOUT_URL mahout
fi


echo "build mahout"
cd  $MAHOUT_HOME/mahout
mvn clean install -DskipTests


echo "add mahout to path"
    tee -a ~/.bash_profile> /dev/null <<EOF
export PATH=$PATH:$MAHOUT_HOME/mahout/bin
EOF
    source ~/.bash_profile
#    export PATH=$PATH:$MAHOUT_HOME/mahout/bin

