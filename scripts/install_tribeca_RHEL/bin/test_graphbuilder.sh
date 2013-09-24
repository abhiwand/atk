#!/bin/bash

GRAPHBUILDER_HOME=$1
pushd

cd $GRAPHBUILDER_HOME/graphbuilder
mvn package >& gb.log
REAL=`grep Failures gb.log | tail -n 1 | sed 's/,/ /g' | awk '{print $5+$7}'` 
if [ $REAL = 0 ]; then
      echo "graphbuilder works fine"
else
      echo "graphbuilder tests failed"
fi

echo " "
popd
