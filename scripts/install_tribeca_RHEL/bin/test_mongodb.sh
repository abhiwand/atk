#!/bin/bash

MONGODB_HOME=$1
MONGODB_VERION=$2
TEST_DIR=`pwd`/../test

pushd  $MONGODB_HOME/mongodb-linux-x86_64-$MONGODB_VERION/bin
./bsondump $TEST_DIR/zips.bson > $TEST_DIR/new-zips.json

result=`diff $TEST_DIR/new-zips.json $TEST_DIR/zips.json | wc -l`
if [ $result = 0 ]; then
    echo "mongodb works fine"
else
    echo "mongodb does not work"
fi

echo " "
popd
