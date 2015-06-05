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

import iatest
iatest.init()

import unittest

from intelanalytics.core.iatypes import *
from intelanalytics.rest.frame import FrameSchema, FrameData

class TestConnect(unittest.TestCase):

    def test_meta(self):
        import intelanalytics as ia
        ia.connect()

class TestInspectionTable(unittest.TestCase):

    def test_get_schema_for_selected_columns(self):
        schema = [('user_id', int32), ('vertex_type', str), ('movie_id', int32), ('rating', int32), ('splits', str)]
        selected_schema = FrameSchema.get_schema_for_columns(schema, ['user_id', 'splits'])
        self.assertEqual(selected_schema, [('user_id', int32), ('splits', str)])

    def test_get_schema_for_selected_columns_change_order(self):
        schema = [('user_id', int32), ('vertex_type', str), ('movie_id', int32), ('rating', int32), ('splits', str)]
        selected_schema = FrameSchema.get_schema_for_columns(schema, ['splits', 'user_id', 'rating'])
        self.assertEqual(selected_schema, [('splits', str), ('user_id', int32), ('rating', int32)])

    def test_get_indices_for_selected_columns(self):
        schema = [('user_id', int32), ('vertex_type', str), ('movie_id', int32), ('rating', int32), ('splits', str)]
        indices = FrameSchema.get_indices_for_selected_columns(schema, ['user_id', 'splits'])
        self.assertEqual(indices, [0, 4])

    def test_extract_columns_from_data(self):
        indices = [0, 2]
        data = [[1, 'a', '3'], [2, 'b', '2'], [3, 'c', '5'], [4, 'd', '-10']]
        result = FrameData.extract_data_from_selected_columns(data, indices)
        self.assertEqual(result, [[1, '3'], [2, '2'], [3, '5'], [4, '-10']])


if __name__ == '__main__':
    unittest.main()
