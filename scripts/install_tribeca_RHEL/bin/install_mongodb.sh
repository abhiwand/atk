#!/bin/bash

MONGODB_URL=$1
MONGODB_HOME=$2
MONGODB_VERSION=$3

cd ~/Downloads
wget $MONGODB_URL/mongodb-linux-x86_64-$MONGODB_VERSION.tgz
tar xvfz mongodb-linux-x86_64-$MONGODB_VERSION.tgz -C $MONGODB_HOME
echo "add mongodb to path"
sudo tee -a /etc/profile.d/gaousr.sh > /dev/null <<THIRD
export PATH=$PATH:$MONGODB_HOME/mongodb-linux-x86_64-$MONGODB_VERSION/bin 
THIRD

source /etc/profile
