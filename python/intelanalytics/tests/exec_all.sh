#!/bin/bash

##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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

# '-x' to exclude dirs from coverage, requires nose-exclude to be installed

export IN_UNIT_TESTS='true'

if [[ -f /usr/lib/IntelAnalytics/virtpy/bin/activate ]]; then
    ACTIVATE_FILE=/usr/lib/IntelAnalytics/virtpy/bin/activate
else
    ACTIVATE_FILE=/usr/local/virtpy/bin/activate
fi

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

TESTS_DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"
INTELANALYTICS_DIR=`dirname $TESTS_DIR`
PYTHON_DIR=`dirname $INTELANALYTICS_DIR`

echo TESTS_DIR=$TESTS_DIR
echo INTELANALYTICS_DIR=$INTELANALYTICS_DIR
echo PYTHON_DIR=$PYTHON_DIR

cd $PYTHON_DIR
#pushd $INTEL_ANALYTICS_HOME > /dev/null

rm -rf $PYTHON_DIR/cover

if [ "$1" = "-x" ] ; then
  EXCLUDE_DIRS_FILE=$TESTS_DIR/cov_exclude_dirs.txt
  if [[ ! -f $EXCLUDE_DIRS_FILE ]]; then
    echo ERROR: -x option: could not find exclusion file $EXCLUDE_DIRS_FILE
    exit 1
  fi
  echo -x option: excluding files from coverage described in $EXCLUDE_DIRS_FILE
  EXCLUDE_OPTION=--exclude-dir-file=$EXCLUDE_DIRS_FILE
fi


nosetests $TESTS_DIR --with-coverage --cover-package=intelanalytics --cover-erase --cover-inclusive --cover-html --with-xunit  --xunit-file=$PYTHON_DIR/nosetests.xml $EXCLUDE_OPTION

success=$?

COVERAGE_ARCHIVE=$PYTHON_DIR/python-coverage.zip

rm *.log 2> /dev/null
rm -rf $COVERAGE_ARCHIVE 
zip -rq $COVERAGE_ARCHIVE .

deactivate

RESULT_FILE=$PYTHON_DIR/nosetests.xml
COVERAGE_HTML=$PYTHON_DIR/cover/index.html

echo 
echo Output File: $RESULT_FILE
echo Coverage Archive: $COVERAGE_ARCHIVE
echo Coverage HTML: file://$COVERAGE_HTML
echo 

unset IN_UNIT_TESTS

if [[ $success == 0 ]] ; then
   echo "Python Tests Successful"
   exit 0
fi
echo "Python Tests Unsuccessful"
exit 1

