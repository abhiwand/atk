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
readme.txt - Intel Analytics Python documentation

Intel Analytics python API auto-generated documentation uses sphinx tools
reading reStructuredText markup in python docstrings

Requirements
============

sphinx  
# pip install sphinx

numpydoc  (sphinx extension)
# pip install numpydoc

(slightly modified scipy theme is included in this package)

Summary 
=======

The source folder contains basic *.rst files which describe the overall
structure of the documentation.  autodoc is used to automagically pull
documentation for the docstrings in the modules.

Usage
=====

(virtpy)[user@host doc]$ ./create-html-public.sh

Then look in the build folder for the generated html.

Note: the many warnings:

   WARNING: toctree contains reference to nonexisting document

are to be expected.


Configuration
=============

source/conf.py

