#!/bin/bash

GRAPHLAB_URL=$1
GRAPHLAB_HOME=$2
GRAPHLAB_VERSION=$3
OPENMPI_URL=$4
OPENMPI_VERSION=$5
ANT_URL=$6
ANT_HOME=$7
ANT_VERSION=$8
USE_INTERNAL=$9
GL_SUBNET_ID=${10}
GL_SUBNET_MASK=${11}
HADOOP_URL=${12}
HADOOP_VERSION=${13}
CFG_DIR=`pwd`/../cfg

echo "install graphlab prerequisites"
sudo yum install -y git make patch gcc gcc-c++ zlib zlib-devel kernel-devel automake autoconf


INSTALLED_OPENMPI=`which mpiexec | grep openmpi | wc -l`
if [ $INSTALLED_OPENMPI = 0 ]; then
     echo "install graphlab prerequisites -- openmpi"
    cd ~/Downloads
    wget $OPENMPI_URL/openmpi-$OPENMPI_VERSION-1.el6.x86_64.rpm
    sudo yum install -y openmpi openmpi-devel
    OPENMPI_HOME=/usr/lib64/openmpi
else
    OPENMPI_HOME=`which mpiexec | sed 's/\/bin\/mpiexec//g'`
fi

echo "update /etc/profile with openmpi info"
sudo tee -a /etc/profile.d/gaousr.sh > /dev/null <<EOF

export PATH=$PATH:$OPENMPI_HOME/bin
export MPI_C_LIBRARIES=$OPENMPI_HOME/lib
export MPI_CXX_LIBRARIES=$OPENMPI_HOME/lib
export MPI_C_INCLUDE_PATH=$OPENMPI_HOME-x86_64
export MPI_CXX_INCLUDE_PATH=$OPENMPI_HOME-x86_64
export GRAPHLAB_SUBNET_ID=$GL_SUBNET_ID
export GRAPHLAB_SUBNET_MASK=$GL_SUBNET_MASK
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$OPENMPI_HOME/lib
EOF
source /etc/profile

HAS_ANT=`which ant|wc -l`
if [ $HAS_ANT = 0 ]; then
   echo "install graphlab prerequisites - ant"
   cd ~/Downloads
   wget $ANT_URL/apache-ant-$ANT_VERSION-bin.tar.gz
   tar xvfz apache-ant-$ANT_VERSION-bin.tar.gz -C $ANT_HOME
    sudo tee -a /etc/profile.d/gaousr.sh> /dev/null <<EOF
export PATH=$PATH:$ANT_HOME/apache-ant-$ANT_VERSION/bin
export ANT_HOME=$ANT_HOME/apache-ant-$ANT_VERSION
EOF
    source /etc/profile
fi


echo "checkout graphlab"
cd $GRAPHLAB_HOME
git clone $GRAPHLAB_URL
cd graphlab
if [ $USE_INTERNAL = "yes" ]; then
    git checkout tribeca
else
    git checkout $GRAPHLAB_VERSION
fi

echo "configure"
cp $CFG_DIR/CMakeLists.txt .
HADOOP="URL $HADOOP_URL/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz"
sed -i '427d' CMakeLists.txt 
sed -i "427i$HADOOP" CMakeLists.txt
./configure
echo "build graphlab"
cd $GRAPHLAB_HOME/graphlab/release/toolkits/graph_analytics
make -j4
echo "build graphlab"
