#!/bin/sh

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

# install_py.sh  (run as root)
#
# Installs necessary dependencies for Tribeca Python
#
# This script checks for Python2.7 and if not found installs it
# "alongside" the current python.  It creates a virtual env for
# Python2.7 and installs all the Intel Analytics Python dependencies there
#
# This script must be able to install as part of an RPM post install script
# Do not use any yum calls as this will break the RPM
# if you have a yum dependency add it to the REQUIRES seciton of the python-intelanalytics.spec


# set exit script on any error
set -e

PYTHON_VIRTUALENV='/usr/lib/IntelAnalytics/virtpy'

me=`basename $0`
hdr="[$me]>> "

# verify superuser privileges
if [[ $EUID -ne 0 ]]; then
   echo "$me must be run as root" 1>&2
   exit 1
fi

if [[ -z $HTTP_PROXY &&  -z $http_proxy ]]; then
   echo "$me requires that the Environment Variable HTTP_PROXY be set"  1>&2
   exit 1
fi

#-----------------------------------------
# PART I - install python 2.7 virtual env
#-----------------------------------------

python_version="2.7.5"
py27="python2.7"
pip27="pip-2.7"

if ! hash $py27 2>/dev/null; then
    echo $hdr Cannot find $py27  
    echo $hdr Installing Python $python_version

    python_dst="/usr"
    py="Python-$python_version"
    pytar="$py.tgz"
    
    echo $hdr Get Python tarball
    wget http://www.python.org/ftp/python/$python_version/$pytar -O $pytar
    tar xzvf $pytar

    pushd $py

    echo $hdr Install development tools
#    yum -y groupinstall "Development tools"

    echo $hdr Install lib dependecies
#    yum -y install zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel

    echo $hdr Configure and make altinstall
    ./configure --prefix=$python_dst
    make && make altinstall

    echo $hdr Install setuptools
    wget https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -O ez_setup.py
    $py27 ez_setup.py

    popd
fi

if ! hash $pip27 2>/dev/null; then
    echo $hdr Install pip
    wget https://raw.github.com/pypa/pip/master/contrib/get-pip.py -O get-pip.py
    $py27 get-pip.py
fi

# get virtual env for 2.7
echo $hdr Install virtualenv
$pip27 install virtualenv

if [ ! -d "$PYTHON_VIRTUALENV"]; then
    mkdir -p $PYTHON_VIRTUALENV
fi

echo $hdr Setup virtualenv for IntelAnalytics
virtualenv -p $py27 $PYTHON_VIRTUALENV


echo $hdr Activate virtualenv
source $PYTHON_VIRTUALENV/bin/activate

#note: to deactivate virtual env, type: deactivate

#--------------------------------------
# PART II - install the py modules
#--------------------------------------

function check {
    # check if the python module can be imported in the venv
    if [ -e $(python -c "import $1") ]; then
        return 0  # true
    else
        return 1  # false
    fi
}

function test {
    # exit if the python module cannot be imported
    if [ ! -e $(python -c "import $1") ]; then
        echo $hdr **Failed to install $1
        exit 1
    fi
}

function ins {
    # check if module is installed, if not, install and test
    if check $1; then
       echo $hdr Install $1
       pip install $1
       test $1
    fi
}

function ins_ignore_virt {
    if check $1; then
        echo $hdr Install $1
        pip install $1
        pkgs=${PYTHON_VIRTUALENV}/lib/python2.7/site-packages
        pushd ${pkgs}
        for f in /usr/lib/python2.7/site-packages/${1}*
        do
            echo $hdr Create symlink to ${f} at ${pkgs}
            ln -sf ${f}
        done
        popd
    fi
}

ins numpy

# scipy has extra dependencies
if check scipy; then
   echo $hdr Install scipy
#   yum -y install blas-devel lapack-devel
   pip install scipy
   test scipy
fi

ins sympy
ins nltk
ins jinja2
ins tornado
ins mrjob

# matplotlib has extra dependencies
if check matplotlib; then
  echo $hdr Install matplotlib
#  yum -y install yum-utils #required for yum-builddep
 # yum-builddep -y python-matplotlib
  ins matplotlib
  test matplotlib
fi

ins ipython
ins pandas
ins bulbs
ins happybase

# zmq has extra dependencies
if check zmq; then
   echo $hdr Install zmq
#   yum -y install libffi-devel
   pip install cffi
   pip install pyzmq
   test zmq
fi
ins pyjavaproperties
ins mock
#nose haas a pip error that ignores the virtenv
ins_ignore_virt nose
ins coverage


# load MathJax into IPython's static folder to avoid CDN problems
ipython profile create
python -c'from IPython.external.mathjax import install_mathjax;
install_mathjax(dest="/usr/lib/IntelAnalytics/virtpy/lib/python2.7/site-packages/IPython/html/static/mathjax")'

# add pydoop to do hdfs, or mapred in python directly
if check pydoop; then
   echo $hdr Install pydoop
#   yum -y install boost-devel
   # ZY: need HADOOP_HOME and JAVA_HOME to build pydoop
   # for 0.5 release, hadoop is at /home/hadoop/IntelAnalytics
   if [ -z "${HADOOP_HOME}" ]; then
        HADOOP_HOME=/home/hadoop/IntelAnalytics/hadoop
   fi
   if [ -z "${JAVA_HOME}" ]; then
        JAVA_HOME=/usr/lib/jvm/java
   fi
   HADOOP_HOME=${HADOOP_HOME} JAVA_HOME=${JAVA_HOME} pip install pydoop
   # ZY: work-around, pydoop installer somehow ignores the virtenv
   pkgs=${PYTHON_VIRTUALENV}/lib/python2.7/site-packages
   ls ${pkgs}/pydoop* &> /dev/null
   if [ $? -ne 0 ]; then
        pushd ${pkgs}
        for f in /usr/lib/python2.7/site-packages/pydoop*
        do
            echo $hdr Create symlink to ${f} at ${pkgs}
            ln -fs ${f}
        done
        popd
   fi
   test pydoop
fi


echo $hdr Python Virtual Environment Installation complete
echo $hdr "To activate enter: 'source $PYTHON_VIRTUALENV/bin/activate'"