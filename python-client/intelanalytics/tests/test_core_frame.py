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
from mock import patch

from intelanalytics import connect
from intelanalytics.core.frame import Frame, _BaseFrame
from intelanalytics.core.column import Column
from intelanalytics.core.files import CsvFile
from intelanalytics.core.iatypes import *


def get_simple_frame_abcde():
    schema = [('A', int32), ('B', int64), ('C', float32), ('D', float64), ('E', str)]
    f = Frame(CsvFile("dummy.csv", schema))
    connect()
    try:
        del _BaseFrame.schema
    except:
        pass
    setattr(f, "schema", schema)
    return f


def get_simple_frame_abfgh():
    schema = [('A', int32),  ('B', int64), ('F', float32), ('G', float64), ('H', str)]
    f = Frame(CsvFile("dummy.csv", schema))
    connect()
    try:
        del _BaseFrame.schema
    except:
        pass
    setattr(f, "schema", schema)
    return f


def fake_download():
    """Mock out download from server, such that connect works offline"""
    return []


@patch('intelanalytics.meta.config.get_frame_backend')
class FrameConstruction(unittest.TestCase):

    def validate_column_names(self, frame, column_names):
        self.assertEqual(len(column_names), len(frame))
        for i in column_names:
            self.assertNotEqual(None,frame[i])

    @patch("intelanalytics.meta.installapi.download_server_commands", fake_download)
    def test_create(self, patched_be):
        connect()
        f = Frame()
        self.assertEqual(0, f._id)
        self.assertEqual(None, f._error_frame_id)

    def test_create_from_csv(self, patched_be):
        connect()
        f = Frame(CsvFile("dummy.csv", [('A', int32), ('B', int64)]))
        self.assertEqual(0, len(f))
        try:
            c = f['C']
            self.fail()
        except KeyError:
            pass

    def test_slice_columns_with_list(self, patched_be):
        f = get_simple_frame_abcde()
        cols = f[['B', 'C']]
        self.assertEqual(2, len(cols))
        self.assertTrue(isinstance(cols[0], Column) and cols[0].name == 'B')
        self.assertTrue(isinstance(cols[1], Column) and cols[1].name == 'C')

    def test_iter(self, x):
        f1 = get_simple_frame_abcde()
        names = [c.name for c in f1]
        self.validate_column_names(f1, names)
