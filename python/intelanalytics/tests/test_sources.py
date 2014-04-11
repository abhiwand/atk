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

from intelanalytics.core.sources import SimpleDataSource
from intelanalytics.core.types import *

expected_repr_123 = """   a      b
0  1    one
1  2    two
2  3  three"""


class TestSimpleDataSource(unittest.TestCase):

    def test_to_pandas_dataframe_using_rows(self):
        sds = SimpleDataSource(schema=[('a', int32), ('b', str)], rows=[(1, 'one'), (2, 'two'), (3, 'three')])
        df = sds.to_pandas_dataframe()
        result = repr(df)
        self.assertEquals(expected_repr_123, result)

    def test_to_pandas_dataframe_using_columns(self):
        sds = SimpleDataSource(schema=[('a', int32), ('b', str)],
                               columns={'a': [1, 2, 3],
                                        'b': ['one', 'two', 'three']})
        df = sds.to_pandas_dataframe()
        result = repr(df)
        self.assertEquals(expected_repr_123, result)


if __name__ == '__main__':
    unittest.main()

