#!/bin/bash
#
# This script executes all of the tests located in this folder through the use
# of the nosetests api.
#

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
PYTHON_DIR='/usr/lib/python2.7/site-packages'
TARGET_DIR=$DIR/target
OUTPUT=$TARGET_DIR/surefire-reports/TEST-nosetests.xml
export PYTHONPATH=$DIR/../python:$PYTHONPATH:$PYTHON_DIR

echo "$NAME DIR=$DIR"
echo "$NAME PYTHON_DIR=$PYTHON_DIR"
echo "$NAME PYTHONPATH=$PYTHONPATH"
echo "$NAME all generated files will go to target dir: $TARGET_DIR"

echo "$NAME Shutting down old API Server if it is still running"
$DIR/api-server-stop.sh


if [ ! -d $TARGET_DIR/surefire-reports/ ]
then
    echo "$NAME Creating target dir"
    mkdir -p $TARGET_DIR/surefire-reports/
fi

echo "$NAME remove old intelanalytics"
rm -rf $TARGET_DIR/fs-root/intelanalytics

echo "$NAME Starting api server... "
$DIR/api-server-start.sh
STARTED=$?
if [[ $STARTED == 2 ]]
then
    echo "$NAME FAILED Couldn't start API server"
    exit 2
fi

PORT=19099
until netstat -atn | grep -q :$PORT
do
    echo "$NAME Waiting for API Server to start up on port $PORT..."
    sleep 1
done

echo "$NAME Calling nosetests"
# parallel execution isn't very parallel because we're hitting 104 Http Errors (probably because we have Spray/Akka configured wrong)
nosetests $DIR/testcases --nologcapture --with-xunit --xunit-file=$OUTPUT --processes=10 --process-timeout=90 --with-isolation
SUCCESS=$?

$DIR/api-server-stop.sh

if [[ $SUCCESS == 0 ]] ; then
   echo "$NAME Python Tests PASSED"
   exit 0
else
   echo "$NAME Python Tests FAILED"
   echo "$NAME see nosetest output: $OUTPUT "
   echo "$NAME also see log output in target dir"
   exit 1
fi