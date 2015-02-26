##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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

from setuptools import setup
import os

setup(
    # Application name:
    name="intelanalytics",

    # Version number (initial):
    version=u"VERSION",

    # Application author details:
    author="Intel",
    author_email="bleh@intel.com",

    # Packages
    packages=["intelanalytics","intelanalytics/core","intelanalytics/rest","intelanalytics/tests"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://analyticstoolkit.intel.com",

    #
    license="LICENSE.txt",
    description="Intel Analytics Toolkit build ID BUILD_NUMBER",

    long_description=open("README").read(),

    # Dependent packages (distributions)
    install_requires=[
        'bottle >= 0.12',
        'numpy >= 1.8.1',
        'requests >= 2.2.1',
        'ordereddict >= 1.1',
        'decorator >= 3.4.0',
        'pandas >= 0.15.0',
    ],
)
