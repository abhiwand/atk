#!/bin/bash

MAHOUT_URL=$1
MAHOUT_HOME=$2
MAHOUT_VERSION=$3
MAVEN_URL=$4
MAVEN_HOME=$5
MAVEN_VERSION=$6
USE_INTERNAL=$7
LAB_MACHINE=$8
CFG_DIR=`pwd`/../cfg


echo "install mahout prerequisites - maven"
HAS_MAVEN=`which mvn | wc -l`
if [ $HAS_MAVEN = 0 ]; then
   echo "install giraph prerequisites - maven"
   source install_maven.sh $MAVEN_HOME $MAVEN_URL $MAVEN_VERSION $CFG_DIR
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

