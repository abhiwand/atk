#!/bin/bash

#This script executes all of the tests located in this folder through the use of the nosetests api. Coverage is provided by coverage.py
#This script requires the installation of the install_pyenv.sh script



ACTIVATE_FILE=/usr/lib/IntelAnalytics/virtpy/bin/activate

if [[ ! -f $ACTIVATE_FILE ]]; then
    echo "Virtual Environment is not installed please execute install_pyenv.sh to install."
    exit 0
fi

source $ACTIVATE_FILE

#check if the python libraries are correctly installed by importing them through python. If there is no output then the module exists.
if [[ -e $(python -c "import nose") ]]; then
    echo "Nosetests is not installed into your python virtual environment please install nose."
    exit 1
fi

if [[ -e $(python -c "import coverage") ]]; then
    echo "Coverage.py is not installed into your python virtual environment please install coverage."
    exit 1
fi

DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"
echo "set the following environment variables to a status needed to execute tests"
echo $INTEL_ANALYTICS_PYTHON
echo $INTEL_ANALYTICS_HOME
echo $SOURCE_CODE

if [[ -z "$INTEL_ANALYTICS_PYTHON" ]]; then
    export INTEL_ANALYTICS_PYTHON=`dirname $DIR`
fi
if [[ -z "$INTEL_ANALYTICS_HOME" ]]; then
    export INTEL_ANALYTICS_HOME=`dirname $INTEL_ANALYTICS_PYTHON`
fi
if [[ -z "$SOURCE_CODE" ]]; then
    export SOURCE_CODE=`dirname $INTEL_ANALYTICS_HOME`
fi

if [[ ! -f $INTEL_ANALYTICS_HOME/conf/intel_analytics.properties ]]; then
    #configuration file does not exist link it to the actual default properties file
    ln -s $INTEL_ANALYTICS_HOME/intel_analytics/intel_analytics.properties $INTEL_ANALYTICS_HOME/conf/intel_analytics.properties
fi

rm -rf $INTEL_ANALYTICS_HOME/cover

nosetests $INTEL_ANALYTICS_PYTHON --with-coverage --cover-package=intel_analytics --cover-erase --cover-inclusive --cover-html --with-xunit  --xunit-file=$INTEL_ANALYTICS_HOME/nosetests.xml

COVERAGE_ARCHIVE=$SOURCE_CODE/python-coverage.zip

rm  $COVERAGE_ARCHIVE

pushd $INTEL_ANALYTICS_HOME/cover
zip -r $COVERAGE_ARCHIVE .
popd

deactivate

RESULT_FILE=$INTEL_ANALYTICS_HOME/nosetests.xml

if grep -q 'failures="0"' "$RESULT_FILE" ; then
   echo "Python Tests Successful"
   exit 0
fi
echo "Python Tests Unsuccessful"
exit 1


