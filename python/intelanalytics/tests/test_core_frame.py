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
#iatest.set_logging("intelanalytics.core.backend", 20)
#iatest.set_logging("intelanalytics.core.frame", 20)
#iatest.set_logging("intelanalytics.core.operations", 20)

import unittest
from mock import patch, Mock

from intelanalytics.core.frame import Frame
from intelanalytics.core.files import CsvFile
from intelanalytics.core.iatypes import *


def get_simple_frame_abcde():
    return Frame(CsvFile("dummy.csv", [('A', int32),
                                          ('B', int64),
                                          ('C', float32),
                                          ('D', float64),
                                          ('E', str)]))


def get_simple_frame_abfgh():
    return Frame(CsvFile("dummy.csv", [('A', int32),
                                          ('B', int64),
                                          ('F', float32),
                                          ('G', float64),
                                          ('H', str)]))

@patch('intelanalytics.meta.config.get_frame_backend')
class FrameConstruction(unittest.TestCase):

    def validate_column_names(self, frame, column_names):
        self.assertEqual(len(column_names), len(frame))
        for i in column_names:
            self.assertNotEqual(None,frame[i])


    @patch("intelanalytics.core.frame.check_api_is_loaded", Mock())
    def test_create(self, get_frame_backend):
        f = Frame()
        self.assertEqual(0, f._id)
        self.assertEqual(None, f._error_frame_id)

    '''

    def test_create_from_csv(self, get_frame_backend):
        f = Frame(CsvFile("dummy.csv", [('A', int32), ('B', int64)]))
        self.assertEqual(0, len(f))
        try:
            c = f['C']
            self.fail()
        except KeyError:
            pass


    def test_slice_columns_with_list(self):
        f = get_simple_frame_abcde()
        cols = f[['B', 'C']]
        self.assertEqual(2, len(cols))
        self.assertTrue(isinstance(cols[0], BigColumn) and cols[0].name == 'B')
        self.assertTrue(isinstance(cols[1], BigColumn) and cols[1].name == 'C')

    def test_del_column(self):
        f = get_simple_frame_abcde()
        self.assertEqual(5, len(f))
        f.drop_columns('B')
        self.assertEqual(4, len(f))
        try:
            b = f['B']
            self.fail()
        except KeyError:
            pass

    def test_del_column_with_list(self):
        f = get_simple_frame_abcde()
        self.assertEqual(5, len(f))
        f.drop_columns(['C', 'E', 'A'])
        self.assertEqual(2, len(f))
        self.assertEqual(2, len(f.column_names))
        self.assertTrue('D' in f.column_names)
        self.assertTrue('B' in f.column_names)

    def test_projection(self):
        f1 = get_simple_frame_abcde()
        f2 = Frame(f1[['B', 'C']])
        self.validate_column_names(f2, ['B', 'C'])

    def test_rename(self):
        f1 = get_simple_frame_abcde()
        f1.rename_columns('A', 'X')
        self.validate_column_names(f1, ['X', 'B', 'C', 'D', 'E'])
        f1.rename_columns('E', 'Y')
        self.validate_column_names(f1, ['X', 'B', 'C', 'D', 'Y'])
        f1.rename_columns('X', 'Z')
        self.validate_column_names(f1, ['Z', 'B', 'C', 'D', 'Y'])

    def test_iter(self):
        f1 = get_simple_frame_abcde()
        names = [c.name for c in f1]
        self.validate_column_names(f1, names)
'''

#@patch('intelanalytics.meta.config.get_frame_backend', new=FrameBackendSimplePrint)
class Debug(unittest.TestCase):
    # container to isolate a test
    pass


if __name__ == '__main__':
    unittest.main()
