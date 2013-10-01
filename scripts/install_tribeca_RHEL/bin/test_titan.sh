#!/bin/bash

TITAN_HOME=$1
pushd

echo "test titan"
cd $TITAN_HOME/titan
mvn test >& titan.log
REAL=`grep Failures titan.log | tail -n 1 | sed 's/,/ /g' | awk '{print $5+$7}'` 
if [ $REAL = 0 ]; then
      echo "titan works fine"
else
      echo "titan tests failed"
fi

echo " "
popd
