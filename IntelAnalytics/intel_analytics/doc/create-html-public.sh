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
set -e

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

# Make sure env vars are set: CLUSTER_HOSTS, HADOOP_HOME, TITAN_HOME, FAUNUS_HOME
set ch = "bogus CLUSTER_HOSTS var set for doc creation"
set hh = "bogus HADOOP_HOME var set for doc creation"
set th = "bogus TITAN_HOME var set for doc creation"
set fh = "bogus FAUNUS_HOME var set for doc creation"

if [ -z "$CLUSTER_HOSTS" ]; then
    export CLUSTER_HOSTS=ch
fi
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP_HOME=hh
fi
if [ -z "$TITAN_HOME" ]; then
    export TITAN_HOME=th
fi
if [ -z "$FAUNUS_HOME" ]; then
    export FAUNUS_HOME=fh
fi

DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"
# see http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in

pushd $DIR

python efuncgen.py > source/efunc.rst
make -B html 2>&1 | grep -v 'WARNING: toctree contains reference to nonexisting document'
zip -rq intel_analytics_pydoc.zip build
echo "make docs directory"
mkdir -p ../../ipython/TemplateOverrides/html/static/docs
echo "copy built html to Template Overrides"
pwd

cp -rv build/html/* ../../ipython/TemplateOverrides/html/static/docs
echo "done copying files "
popd

# undo if we made any changes above
if [ "$CLUSTER_HOSTS" == ch ]; then
    unset CLUSTER_HOSTS
fi
if [ "$HADOOP_HOME" == hh ]; then
    unset HADOOP_HOME
fi
if [ "$TITAN_HOME" == th ]; then
    unset TITAN_HOME
fi
if [ "$FAUNUS_HOME" == th ]; then
    unset FAUNUS_HOME
fi
