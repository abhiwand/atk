#!/bin/bash
#
# This script executes all of the tests located in this folder through the use
# of the nosetests api.
#

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
PYTHON_DIR='/usr/lib/python2.7/site-packages'
TARGET_DIR=$DIR/target
OUTPUT1=$TARGET_DIR/surefire-reports/TEST-nose-smoketests.xml
OUTPUT2=$TARGET_DIR/surefire-reports/TEST-nose-tests.xml
export PYTHONPATH=$DIR/../python-client:$PYTHONPATH:$PYTHON_DIR

echo "$NAME DIR=$DIR"
echo "$NAME PYTHON_DIR=$PYTHON_DIR"
echo "$NAME PYTHONPATH=$PYTHONPATH"
echo "$NAME all generated files will go to target dir: $TARGET_DIR"

echo "$NAME Shutting down old API Server if it is still running"
$DIR/rest-server-stop.sh


if [ ! -d $TARGET_DIR/surefire-reports/ ]
then
    echo "$NAME Creating target dir"
    mkdir -p $TARGET_DIR/surefire-reports/
fi

echo "$NAME remove old intelanalytics"
rm -rf $TARGET_DIR/fs-root/intelanalytics

echo "$NAME Starting REST server... "
$DIR/rest-server-start.sh
STARTED=$?
if [[ $STARTED == 2 ]]
then
    echo "$NAME FAILED Couldn't start REST server"
    exit 2
fi

sleep 10
PORT=19099
COUNTER=0
httpCode=$(curl -s -o /dev/null -w "%{http_code}" localhost:$PORT)
echo $httpCode
while [ $httpCode != "200" ]
do
    if [ $COUNTER -gt 120 ]
    then
        echo "$NAME Tired of waiting for REST Server to start up, giving up..."
        $DIR/rest-server-stop.sh
        exit 3
    else
        let COUNTER=COUNTER+1
    fi
    echo "$NAME Waiting for REST Server to start up on port $PORT..."
    sleep 1
    httpCode=$(curl -s -o /dev/null -w "%{http_code}" localhost:$PORT)
    echo $httpCode
done

echo "$NAME nosetests will be run in two calls: 1) make sure system works in basic way, 2) the rest of the tests"
sleep 10
# Rene said each build agent has about 18 cores (Feb 2015)

echo "$NAME Running smoke tests to verify basic functionality needed by all tests, calling nosetests"
nosetests $DIR/smoketests --nologcapture --with-xunitmp --xunitmp-file=$OUTPUT1 --processes=10 --process-timeout=300 --with-isolation
SMOKE_TEST_SUCCESS=$?

if [[ $SMOKE_TEST_SUCCESS == 0 ]] ; then
    echo "$NAME Python smoke tests PASSED -- basic frame,graph,model functionality verified"
    echo "$NAME Running the rest of the tests, calling nosetests again"
    nosetests $DIR/tests --nologcapture --with-xunitmp --xunitmp-file=$OUTPUT2 --processes=10 --process-timeout=300 --with-isolation
    TEST_SUCCESS=$?
fi

$DIR/rest-server-stop.sh

if [[ $SMOKE_TEST_SUCCESS != 0 ]] ; then
   echo "$NAME Python smoke tests FAILED"
   echo "$NAME bailing out early, no reason to run any other tests if smoke tests are failing"
   echo "$NAME see nosetest output: $OUTPUT1"
   echo "$NAME also see log output in target dir"
   exit 1
elif [[ $TEST_SUCCESS == 0 ]] ; then
   echo "$NAME All Python tests PASSED"
   exit 0
else
   echo "$NAME Python tests FAILED"
   echo "$NAME see nosetest output: $OUTPUT2 "
   echo "$NAME also see log output in target dir"
   exit 2
fi

