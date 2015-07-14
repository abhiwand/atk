#
# Copyright (c) 2015 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import iatest
iatest.init()

import unittest
import taprootanalytics as ta
from taprootanalytics.core.ui import _get_col_sizes, _get_num_cols, _get_row_clump_count

f_schema = [('i32', ta.int32),
            ('floaties', ta.float64),
            ('s', str),
            ('long_column_name_ugh_and_ugh', str),
            ('long_value', str)]

f_rows = [
    [1,
     3.14159265358,
     'one',
     'a',
     '''The sun was shining on the sea,
Shining with all his might:
He did his very best to make
The billows smooth and bright--
And this was odd, because it was
The middle of the night.

The moon was shining sulkily,
Because she thought the sun
Had got no business to be there
After the day was done--
"It's very rude of him," she said,
"To come and spoil the fun!"'''],
    [2,
     8.014512183,
     'two',
     'b',
     '''I'm going down.  Down, down, down, down, down.  I'm going down.  Down, down, down, down, down.  I'm going down.  Down, down, down, down, down.  I'm going down.  Down, down, down, down, down.'''],
    [32,
     1.0,
     'thirty-two',
     'c',
     'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA']]

#wrap
#expected = [3, 13, 10, 28, 31]

schema = [('a', int), ('b', unicode), ('c', unicode)]
rows = [[1, "sixteen_16_abced", "long"],
        [2, "tiny", "really really really really long"]]

class TestConnect(unittest.TestCase):

    def test_get_col_sizes1(self):
        result = _get_col_sizes(rows, 0, schema, wrap=4)
        expected = [7, 16,  32]
        self.assertEquals(expected, result)

    def go_get_num_cols(self, width, expected):
        result = _get_col_sizes(rows, 0, schema, wrap=2)
        def get_splits(width):
            num_cols_0 = _get_num_cols(schema, width, 0, result)
            num_cols_1 = _get_num_cols(schema, width, num_cols_0, result)
            num_cols_2 = _get_num_cols(schema, width, num_cols_0 + num_cols_1, result)
            return num_cols_0, num_cols_1, num_cols_2

        self.assertEquals(expected, get_splits(width))

    def test_get_num_cols_12(self):
        self.go_get_num_cols(12, (1, 1, 1))

    def test_get_num_cols_24(self):
        self.go_get_num_cols(24, (2, 1, 0))

    def test_get_num_cols_80(self):
        self.go_get_num_cols(80, (3, 0, 0))

    def test_get_row_clump_count(self):
        row_count=12
        wraps = [(12, 1), (11, 2), (10, 2), (6, 2), (5, 3), (4, 3), (3, 4), (2, 6), (1, 12), (13, 1), (100, 1)]
        for w in wraps:
            wrap = w[0]
            expected = w[1]
            result = _get_row_clump_count(row_count, wrap)
            self.assertEqual(expected, result, "%s != %s for wrap %s" % (result, expected, wrap))


if __name__ == '__main__':
    unittest.main()