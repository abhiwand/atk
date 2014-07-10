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
import iatest
iatest.init()

import unittest
from intelanalytics.core.iatypes import *


class SupportedTypes(unittest.TestCase):

    def test_supported_types(self):
        self.assertTrue(ignore not in supported_types)
        self.assertTrue(string in supported_types)
        self.assertTrue(int32 in supported_types)
        self.assertTrue(int not in supported_types)
        self.assertTrue(float not in supported_types)

    def test_supported_types_repr(self):
        self.assertEqual(len(supported_types.__repr__().split(',')),
                         len(supported_types))

if __name__ == '__main__':
    unittest.main()
