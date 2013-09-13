#!/bin/bash

HBASE_URL=$1
HBASE_HOME=$2
HBASE_VERSION=$3
HADOOP_HOME=$4
HADOOP_VERSION=$5
MAVEN_URL=$6
MAVEN_HOME=$7
MAVEN_VERSION=$8
LAB_MACHINE=$9
MAVEN_REPO_DIR=${10}
CFG_DIR=`pwd`/../cfg

echo "install hbase prerequisites - maven"
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
#    sudo chown -R  root:root $MAVEN_HOME/apache-maven-$MAVEN_VERSION/bin
fi



echo "download hbase"
cd ~/Downloads
wget $HBASE_URL/hbase-$HBASE_VERSION/hbase-$HBASE_VERSION.tar.gz
tar xvfz hbase-$HBASE_VERSION.tar.gz -C $HBASE_HOME

echo "modify hbase-env.sh JAVA_HOME & HBASE_CLASSPATH"
cd $HBASE_HOME/hbase-$HBASE_VERSION/conf
INSTALLED_JAVA=`egrep -v "^#|^$"   $HADOOP_HOME/hadoop-$HADOOP_VERSION/conf/hadoop-env.sh  | grep  JAVA_HOME`

cp $CFG_DIR/hbase-env.sh .
sed -i '25d' hbase-env.sh
sed -i "25i$INSTALLED_JAVA" hbase-env.sh

HBASE_CLASSPATH="export HBASE_CLASSPATH=$HBASE_HOME/hbase-$HBASE_VERSION/lib"
sed -i '28d' hbase-env.sh
sed -i "28i$HBASE_CLASSPATH" hbase-env.sh


echo "modify hbase-site.xml hbase.rootdir  & hbase.zookeeper.quorum"
cp $CFG_DIR/hbase-site.xml .
ROOTDIR="     <value>hdfs://"`hostname`":19010/hbase</value>"
sed -i '27d' hbase-site.xml
sed -i "27i$ROOTDIR" hbase-site.xml

QUORUM="     <value>"`hostname`"</value>"
sed -i '35d' hbase-site.xml
sed -i "35i$QUORUM" hbase-site.xml


echo "modify hadoop-env.sh  HADOOP_CLASSPATH"
HADOOP_CLASSPATH="export HADOOP_CLASSPATH=$HBASE_HOME/hbase-$HBASE_VERSION/hbase-$HBASE_VERSION.jar"
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:"$HBASE_HOME/hbase-$HBASE_VERSION/hbase-$HBASE_VERSION-tests.jar"
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:"$HBASE_HOME/hbase-$HBASE_VERSION/conf"
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:"$HBASE_HOME/hbase-$HBASE_VERSION/lib"
cd $HBASE_HOME/hbase-$HBASE_VERSION/lib
ZK=`ls zookeeper*jar`
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:"$HBASE_HOME/hbase-$HBASE_VERSION/lib/$ZK"
PB=`ls protobuf*jar`
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:"$HBASE_HOME/hbase-$HBASE_VERSION/lib/$BP"
GUAVA=`ls guava*jar`
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:"$HBASE_HOME/hbase-$HBASE_VERSION/lib/$GUAVA"

sed -i '12d' $HADOOP_HOME/hadoop-$HADOOP_VERSION/conf/hadoop-env.sh
sed -i "12i$HADOOP_CLASSPATH" $HADOOP_HOME/hadoop-$HADOOP_VERSION/conf/hadoop-env.sh


echo "copy files from hbase to hadoop"
cp $HBASE_HOME/hbase-$HBASE_VERSION/hbase-$HBASE_VERSION.jar $HADOOP_HOME/hadoop-$HADOOP_VERSION/lib/
cp $HBASE_HOME/hbase-$HBASE_VERSION/lib/zookeeper-*.jar $HADOOP_HOME/hadoop-$HADOOP_VERSION/lib/
cp $HBASE_HOME/hbase-$HBASE_VERSION/lib/protobuf-*.jar /$HADOOP_HOME/hadoop-$HADOOP_VERSION/lib/
cp $HBASE_HOME/hbase-$HBASE_VERSION/lib/guava-*.jar /$HADOOP_HOME/hadoop-$HADOOP_VERSION/lib/

echo "copy file from hadoop to hbase"
cp $HADOOP_HOME/hadoop-$HADOOP_VERSION/hadoop-core-$HADOOP_VERSION.jar $HBASE_HOME/hbase-$HBASE_VERSION/lib/

echo "add hbase to path"
   tee -a ~/.bash_profile> /dev/null <<EOF
export PATH=$PATH:$HBASE_HOME/hbase-$HBASE_VERSION/bin
EOF
   source ~/.bash_profile

#   sudo chown -R  root:root $HBASE_HOME/hbase-$HBASE_VERSION/bin

