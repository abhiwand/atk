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

from intelanalytics.little.frame import LittleFrame
from intelanalytics.core.types import *
from intelanalytics.core.sources import SimpleDataSource



schema_ab = [('a', int32), ('b', str)]
rows_ab_123 =[(1, 'one'), (2, 'two'), (3, 'three')]
data_source_ab_123 = SimpleDataSource(schema=schema_ab, rows=rows_ab_123)
expected_repr_ab_123 = """  a:int32  b:str
        1    one
        2    two
        3  three"""

schema_nenfr = [('n', int32), ('en', str), ('fr', str)]
rows_nenfr =  [(1, 'one', "un"),
               (2, 'two', None),
               (3, None, 'trois'),
               (4, None, None),
               (None, 'five', 'cinq'),
               (None, 'six', None),
               (None, None, 'sept'),
               (None, None, None),
               (9, 'nine', 'neuf')]
data_source_nenfr_na = SimpleDataSource(schema=schema_nenfr, rows=rows_nenfr)

class TestLittleFrame(unittest.TestCase):

    def validate_schema(self, frame, schema):
        frame_schema = frame.schema
        self.assertEquals(len(schema), len(frame_schema))
        for n, t in schema:
            self.assertEquals(frame_schema[n], t)

    def validate_rows(self, frame, rows):
        for i, r in enumerate(frame.take(len(rows))):
            self.assertEquals(rows[i], r)

    def validate_drop(self, frame, src_rows, cases):
        self.assertEquals(len(cases), frame.count())
        for i, r in enumerate(frame.take(100)):
            self.assertEquals(src_rows[cases[i]], r)

    def validate_drop_ab_123(self, frame, cases):
        self.validate_drop(frame, rows_ab_123, map(lambda x: x-1, cases))

    def validate_drop_nenfr(self, frame, cases):
        self.validate_drop(frame, rows_nenfr, map(lambda x: x-1, cases))

    def test_simple_creation_and_repr(self):
        f = LittleFrame(data_source_ab_123)
        r = repr(f)
        #print r
        self.assertEquals(expected_repr_ab_123, r)

    def test_take(self):
        f = LittleFrame(data_source_nenfr_na)
        rows = f.take(10)
        self.assertEquals(9, len(rows))
        for i, r in enumerate(rows):
            self.assertEquals(rows_nenfr[i], r)

    def test_take_roundtrip(self):
        f = LittleFrame(data_source_nenfr_na)
        g = LittleFrame(SimpleDataSource(schema=f.schema, rows=f.take(100)))
        self.assertEquals(repr(f), repr(g))

    def test_drop(self):
        f = LittleFrame(data_source_ab_123)
        self.assertEquals(3, f.count())
        f.drop(lambda row: row.a == 2)
        self.validate_drop_ab_123(f, [1, 3])
        f.drop(lambda row: row['a'] == 3)
        self.validate_drop_ab_123(f, [1])

    def test_filter(self):
        f = LittleFrame(data_source_ab_123)
        f.filter(lambda x: x.a == 2)
        self.validate_drop_ab_123(f, [2])

    def test_dropna_no_args(self):
        f = LittleFrame(data_source_nenfr_na)
        f.dropna()
        self.validate_drop_nenfr(f, [1, 9])

    def test_dropna_all(self):
        f = LittleFrame(data_source_nenfr_na)
        f.dropna(all)
        self.validate_drop_nenfr(f, [1, 2, 3, 4, 5, 6, 7, 9])

    def test_dropna_a(self):
        f = LittleFrame(data_source_nenfr_na)
        f.dropna('n')
        self.validate_drop_nenfr(f, [1, 2, 3, 4, 9])

    def test_dropna_any_subset(self):
        f = LittleFrame(data_source_nenfr_na)
        f.dropna(any, ['en', 'fr'])
        self.validate_drop_nenfr(f, [1, 5, 9])

    def test_dropna_all_subset(self):
        f = LittleFrame(data_source_nenfr_na)
        f.dropna(all, ['n', 'en'])
        self.validate_drop_nenfr(f, [1, 2, 3, 4, 5, 6, 9])

    # TODO - repair add column in Little Frame
    # def test_add_column_1(self):
    #     f = LittleFrame(data_source_ab_123)
    #     f.add_column('a_new', lambda r: r['a'] * 3, int32)
    #     #print repr(f)
    #     self.validate_schema(f, [('a', int32), ('b', str), ('a_new', int64)])
    #     self.validate_rows(f, [(1, 'one', 3), (2, 'two', 6), (3, 'three', 9)])
    #
    # def test_add_column_2(self):
    #     f = LittleFrame(data_source_ab_123)
    #     f.add_column(('a3', 'bnew'), lambda r: (r['a'] * 3, r['b'] + '_new'))
    #     #print repr(f)

    def test_get_names_from_schema(self):
        from intelanalytics.little.frame import _get_names_from_schema
        self.assertEquals(('x',), _get_names_from_schema('x'))
        self.assertEquals(('x',), _get_names_from_schema(('x',)))
        self.assertEquals(('x',), _get_names_from_schema(('x', int32)))
        self.assertEquals(('x', 'y'), _get_names_from_schema(('x', 'y')))
        self.assertEquals(('x', 'y'), _get_names_from_schema((('x', int32), 'y')))
        self.assertEquals(('x', 'y', 'z'), _get_names_from_schema((('x', int32), 'y', ('z', int32))))
        self.assertEquals(('x', 'y', 'z'), _get_names_from_schema((('x', int32), ('y', str), ('z', int32))))

    def test_get_pairs_from_schema(self):
        from intelanalytics.little.frame import _get_pairs_from_column_schema
        self.assertEquals([('x', unknown)], _get_pairs_from_column_schema('x'))
        self.assertEquals([('x', unknown)], _get_pairs_from_column_schema(('x',)))
        self.assertEquals([('x', int32)], _get_pairs_from_column_schema(('x', int32)))
        self.assertEquals([('x', unknown), ('y', unknown)], _get_pairs_from_column_schema(('x', 'y')))
        self.assertEquals([('x', int32), ('y', unknown)], _get_pairs_from_column_schema((('x', int32), 'y')))
        self.assertEquals([('x', int32), ('y', unknown), ('z', int32)], _get_pairs_from_column_schema((('x', int32), 'y', ('z', int32))))
        self.assertEquals([('x', int32), ('y', str), ('z', int32)], _get_pairs_from_column_schema((('x', int32), ('y', str), ('z', int32))))

    def test_rename_column(self):
        f = LittleFrame(data_source_ab_123)
        f.rename_column('a', 'a_prime')
        self.validate_schema(f, [('a_prime', int32), ('b', str)])
        self.validate_rows(f, [(1, 'one'), (2, 'two'), (3, 'three')])

    def test_rename_column_multiple_names(self):
        f = LittleFrame(data_source_nenfr_na)
        f.rename_column(('en', 'fr'), ('english', 'french'))
        self.validate_schema(f, [('n', int32), ('english', str), ('french', str)])

    def test_remove_column(self):
        f = LittleFrame(data_source_nenfr_na)
        f.remove_column('en')
        self.validate_schema(f, [('n', int32), ('fr', str)])

    def test_remove_column_multiple(self):
        f = LittleFrame(data_source_nenfr_na)
        f.remove_column(['fr', 'en'])
        self.validate_schema(f, [('n', int32)])
