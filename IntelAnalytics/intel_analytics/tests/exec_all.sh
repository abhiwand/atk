#!/bin/bash

##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

#This script executes all of the tests located in this folder through the use of the nosetests api. Coverage is provided by coverage.py
#This script requires the installation of the install_pyenv.sh script



ACTIVATE_FILE=/usr/lib/IntelAnalytics/virtpy/bin/activate

if [[ ! -f $ACTIVATE_FILE ]]; then
    echo "Virtual Environment is not installed please execute install_pyenv.sh to install."
    exit 1
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

PYTHON_HOME=`dirname $DIR`

export INTEL_ANALYTICS_PYTHON=`dirname $PYTHON_HOME`
export INTEL_ANALYTICS_HOME=$INTEL_ANALYTICS_PYTHON
export SOURCE_CODE=`dirname $INTEL_ANALYTICS_HOME`
export IN_UNIT_TESTS='true'

pushd $INTEL_ANALYTICS_HOME

if [[ ! -f $INTEL_ANALYTICS_HOME/conf/intel_analytics.properties ]]; then
    #configuration file does not exist link it to the actual default properties file
    ln -s $INTEL_ANALYTICS_HOME/intel_analytics/intel_analytics.properties $INTEL_ANALYTICS_HOME/conf/intel_analytics.properties
fi

rm -rf $INTEL_ANALYTICS_HOME/cover

nosetests $PYTHON_HOME --with-coverage --cover-package=intel_analytics --cover-erase --cover-inclusive --cover-html --with-xunit  --xunit-file=$INTEL_ANALYTICS_HOME/nosetests.xml

COVERAGE_ARCHIVE=$SOURCE_CODE/python-coverage.zip

rm *.log

popd

rm  $COVERAGE_ARCHIVE

pushd $INTEL_ANALYTICS_HOME/cover
zip -r $COVERAGE_ARCHIVE .
popd

deactivate

RESULT_FILE=$INTEL_ANALYTICS_HOME/nosetests.xml

unset IN_UNIT_TESTS

if grep -q 'failures="0"' "$RESULT_FILE" ; then
   echo "Python Tests Successful"
   exit 0
fi
echo "Python Tests Unsuccessful"
exit 1


