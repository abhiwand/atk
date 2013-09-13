#!/bin/bash

FAUNUS_HOME=$1
CURRENT_DIR=`pwd`


echo "test faunus"
cd $FAUNUS_HOME/faunus
mvn test >& faunus.log
REAL=`grep Failures faunus.log | tail -n 1 | sed 's/,/ /g' | awk '{print $5+$7}'` 
if [ $REAL = 0 ]; then
      echo "faunus works fine"
else
      echo "faunus tests failed"
fi

echo " "
cd $CURRENT_DIR
