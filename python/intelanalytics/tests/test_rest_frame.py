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
#iatest.set_logging("intelanalytics.rest.connection", 20)

import unittest
from collections import OrderedDict
from intelanalytics.core.types import *
from intelanalytics.rest.frame import FrameBackendREST


class TestInspectionTable(unittest.TestCase):

    def test_inspect(self):
        schema = OrderedDict([('i', int32), ('f', float32), ('en', str), ('fr', str)])
        rows = [[u'1', u' 1.0', u" 'one'", u" 'un'"],
                [u'2', u' 2.0', u" 'two'", u" 'deux'"],
                [u'3', u' 3.0', u" 'three'", u" 'trois'"]]
        it = FrameBackendREST.InspectionTable(schema, rows)
        expected = """i:int32   f:float32    en:str     fr:str
=========================================
      1       1.000    'one'      'un'
      2       2.000    'two'      'deux'
      3       3.000    'three'    'trois'""".replace(" ", "")
        # print repr(it)
        self.assertEquals(expected, repr(it).replace(" ", ""))


if __name__ == '__main__':
    unittest.main()
