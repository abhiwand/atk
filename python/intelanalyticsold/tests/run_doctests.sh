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


# Executes all of the tests defined in this doc folder, using Python's doctest

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

TESTS_DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"
IA=`dirname $TESTS_DIR`
DOC=$IA/doc
SOURCE_CODE_PYTHON=`dirname $IA`

echo SOURCE_CODE_PYTHON=$SOURCE_CODE_PYTHON

cd $SOURCE_CODE_PYTHON 

python -m doctest -v $DOC/source/examples.rst

success=$?

rm *.log 2> /dev/null

if [[ $success == 0 ]] ; then
   echo "Python Doc Tests Successful"
   exit 0
fi
echo "Python Doc Tests Unsuccessful"
exit 1

