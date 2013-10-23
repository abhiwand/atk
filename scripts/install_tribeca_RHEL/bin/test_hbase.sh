#!/bin/bash


HBASE_HOME=$1
HBASE_VERSION=$2

echo "test hbase"
pushd $HBASE_HOME/hbase-$HBASE_VERSION
mvn test  >& hbase.log
REAL=`grep Failures hbase.log | tail -n 1 | sed 's/,/ /g' | awk '{print $5}'` 

if [[ $HBASE_VERSION = "0.94.1" ||$HBASE_VERSION = "0.95.1" ]]; then
   EXPECTED="Tests run: 562, Failures: 2, Errors: 0, Skipped: 0"
   if [ $REAL = 2 ]; then
         echo "hbase works fine"
   fi
elif [ $HBASE_VERSION = "0.94.7"  ]; then
    if [ $REAL = 1 ]; then
          echo "hbase works fine"
    fi
else
          echo "hbase does not work"
fi

echo " "

popd
