#!/bin/bash

#This script executes all of the tests located in this folder through the use of the nosetests api. Coverage is provided by coverage.py
#This script requires the installation of the install_pyenv.sh script

ACTIVATE_FILE=/usr/lib/IntelAnalytics/virtpy/bin/activate

if [[ ! -f $ACTIVATE_FILE ]]; then
    echo "Virtual Environment is not installed please execute install_pyenv.sh to install."
    exit 0
fi

DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"
#set the following environment variables to a status needed to execute tests
export INTEL_ANALYTICS_HOME=$DIR/../..
export INTEL_ANALYTICS_PYTHON=$INTEL_ANALYTICS_HOME/intel_analytics


source $ACTIVATE_FILE

nosetests $DIR --with-coverage --cover-package=intel_analytics --cover-erase --cover-html --with-xunit  --xunit-file=$INTEL_ANALYTICS_HOME/nosetests.xml
rm  $INTEL_ANALYTICS_HOME/python-coverage.zip
zip -r  $INTEL_ANALYTICS_HOME/python-coverage.zip $INTEL_ANALYTICS_HOME/cover


deactivate

RESULT_FILE=$INTEL_ANALYTICS_HOME/nosetests.xml

if grep -q 'failures="0"' "$RESULT_FILE" ; then
   echo "Python Tests Successful"
   exit 0
fi
echo "Python Tests Unsuccessful"
exit 1


