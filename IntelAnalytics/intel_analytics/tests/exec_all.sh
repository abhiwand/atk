#!/bin/bash

#This script executes all of the tests located in this folder through the use of the nosetests api. Coverage is provided by coverage.py
#This script requires the installtion of the install_pyenv.sh script

ACTIVATE_FILE=/usr/lib/IntelAnalytics/virtpy/bin/activate

if [[ ! -f $ACTIVATE_FILE ]]; then
    echo "Virtual Environment is not installed please execute install_pyenv.sh to install."
    exit 0
fi

DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"

source $ACTIVATE_FILE

nosetests $DIR --with-coverage --cover-package=intel_analytics --cover-erase --cover-html --with-xunit  --xunit-file=$DIR/../../nosetests.xml
rm  $DIR/../../python-coverage.zip
zip -r  $DIR/../../python-coverage.zip $DIR/../../cover


deactivate

RESULT_FILE=$DIR/../../nosetests.xml

if grep -q 'failures="0"' "$RESULT_FILE" ; then
   echo "Python Tests Successful"
   exit 0
fi
echo "Python Tests Unsuccessful"
exit 1


