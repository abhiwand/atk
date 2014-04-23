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
"""
Initialization for any unit test
"""
import os
import sys
import logging


class TestFolders(object):
    """Folder paths for the tests"""
    def __init__(self):
        dirname = os.path.dirname
        self.here = dirname(__file__)
        self.tmp = os.path.join(self.here, "tmp")
        self.conf = os.path.join(self.here, "conf")
        self.root = dirname(dirname(self.here))  # parent of intel_analytics

    def __repr__(self):
        return '{' + ",".join(['"%s": "%s"' % (k, v)
                               for k, v in self.__dict__.items()]) + '}'


folders = TestFolders()


def init():
    if sys.path[1] != folders.root:
        sys.path.insert(1, folders.root)


def set_logging(logger_name, level=logging.DEBUG):
    """Sets up logging for the test"""
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    h = logging.StreamHandler()
    h.setLevel(logging.DEBUG)
    logger.addHandler(h)
