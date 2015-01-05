#!/bin/bash
#
# Remove generated artifacts
#

DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "making sure the API server was shutdown"
$DIR/api-server-stop.sh

echo "removing .pyc files"
rm -f testcases/*.pyc

echo "Removing $DIR/target"
rm -rf $DIR/target