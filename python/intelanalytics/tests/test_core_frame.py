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
from mock import patch

from intelanalytics.core.frame import BigFrame
from intelanalytics.core.column import BigColumn
from intelanalytics.core.files import CsvFile
from intelanalytics.core.types import *
from intelanalytics.core.backend import FrameBackendSimplePrint


def get_simple_frame_abcde():
    return BigFrame(CsvFile("dummy.csv", [('A', int32),
                                          ('B', int64),
                                          ('C', float32),
                                          ('D', float64),
                                          ('E', str)]))


def get_simple_frame_abfgh():
    return BigFrame(CsvFile("dummy.csv", [('A', int32),
                                          ('B', int64),
                                          ('F', float32),
                                          ('G', float64),
                                          ('H', str)]))

#@unittest.skip("Debugging")
@patch('intelanalytics.core.config.get_frame_backend', new=FrameBackendSimplePrint)
class FrameConstruction(unittest.TestCase):

    def validate_column_names(self, frame, column_names):
        self.assertEqual(len(column_names), len(frame))
        for i in column_names:
            self.assertIsNotNone(frame[i])

    @patch('intelanalytics.core.frame.BigFrame._get_new_frame_name')
    def test_create(self, get_new_frame_name):
        get_new_frame_name.return_value = 'untitled_1234'
        f = BigFrame()
        self.assertEqual('untitled_1234', f.name)

    def test_create_from_csv(self):
        f = BigFrame(CsvFile("dummy.csv", [('A', int32), ('B', int64)]))
        self.assertEqual(2, len(f))
        self.assertTrue(isinstance(f['A'], BigColumn))
        self.assertTrue(isinstance(f['B'], BigColumn))
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
        f.remove_column('B')
        self.assertEqual(4, len(f))
        try:
            b = f['B']
            self.fail()
        except KeyError:
            pass

    def test_del_column_with_list(self):
        f = get_simple_frame_abcde()
        self.assertEqual(5, len(f))
        f.remove_column(['C', 'E', 'A'])
        self.assertEqual(2, len(f))
        self.assertEqual(2, len(f.column_names))
        self.assertTrue('D' in f.column_names)
        self.assertTrue('B' in f.column_names)

    def test_projection(self):
        f1 = get_simple_frame_abcde()
        f2 = BigFrame(f1[['B', 'C']])
        self.validate_column_names(f2, ['B', 'C'])

    def test_rename(self):
        f1 = get_simple_frame_abcde()
        f1.rename_column('A', 'X')
        self.validate_column_names(f1, ['X', 'B', 'C', 'D', 'E'])
        f1.rename_column('E', 'Y')
        self.validate_column_names(f1, ['X', 'B', 'C', 'D', 'Y'])
        f1.rename_column('X', 'Z')
        self.validate_column_names(f1, ['Z', 'B', 'C', 'D', 'Y'])

    def test_iter(self):
        f1 = get_simple_frame_abcde()
        names = [c.name for c in f1]
        self.validate_column_names(f1, names)


@patch('intelanalytics.core.config.get_frame_backend', new=FrameBackendSimplePrint)
class Debug(unittest.TestCase):
    # container to isolate a test
    pass


if __name__ == '__main__':
    unittest.main()
