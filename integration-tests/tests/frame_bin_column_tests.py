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

import unittest
import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class FrameCopyTest(unittest.TestCase):
    """
    Test all frame binning methods. using the flattenable dataset. all tests bin the number column
    number column goes from 1 to 10

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    def setUp(self):
        print "define csv file"
        csv = ia.CsvFile("/datasets/flattenable.csv", schema= [('number', ia.int32),
                                                               ('abc', str),
                                                               ('food', str)], delimiter=',')

        print "create frame"
        self.frame = ia.Frame(csv)

    def test_lower_inclusive_strict_binning(self):
        self.frame.bin_column('number', [3, 5, 8], include_lowest=True, strict_binning=True)
        results = self.frame.take(100)
        for row in results:
            if row[0] < 3 or row[0] > 8:
                self.assertEqual(row[3], -1)
            if row[0] in [3, 4]:
                self.assertEqual(row[3], 0)
            if row[0] in [5, 6, 7, 8]:
                self.assertEqual(row[3], 1)

    def test_lower_inclusive_eager_binning(self):
        self.frame.bin_column('number', [3, 5, 8], include_lowest=True, strict_binning=False)
        results = self.frame.take(100)
        for row in results:
            if row[0] < 5:
                self.assertEqual(row[3], 0)
            if row[0] >= 5:
                self.assertEqual(row[3], 1)

    def test_upper_inclusive_strict_binning(self):
        self.frame.bin_column('number', [3, 5, 8], include_lowest=False, strict_binning=True)
        results = self.frame.take(100)
        for row in results:
            if row[0] < 3 or row[0] > 8:
                self.assertEqual(row[3], -1)
            if row[0] in [3, 4, 5]:
                self.assertEqual(row[3], 0)
            if row[0] in [6, 7, 8]:
                self.assertEqual(row[3], 1)

    def test_upper_inclusive_eager_binning(self):
        self.frame.bin_column('number', [3, 5, 8], include_lowest=False, strict_binning=False)
        results = self.frame.take(100)
        for row in results:
            if row[0] <= 5:
                self.assertEqual(row[3], 0)
            if row[0] > 5:
                self.assertEqual(row[3], 1)

    def test_bin_equal_width(self):
        cutoffs = self.frame.bin_column_equal_width('number', num_bins=3)
        self.assertEqual(cutoffs, [1, 4, 7, 10])
        results = self.frame.take(100)
        for row in results:
            if row[0] in [1, 2, 3]:
                self.assertEqual(row[3], 0)
            if row[0] in [4, 5, 6]:
                self.assertEqual(row[3], 1)
            if row[0] in [7, 8, 9, 10]:
                self.assertEqual(row[3], 2)

    def test_bin_equal_depth(self):
        cutoffs = self.frame.bin_column_equal_depth('number', num_bins=2)
        self.assertEqual(cutoffs, [1, 6, 10])
        results = self.frame.take(100)
        for row in results:
            if row[0] <= 5:
                self.assertEqual(row[3], 0)
            if row[0] > 5:
                self.assertEqual(row[3], 1)

    def test_default_num_bins(self):
        # the default number of bins is the square-root floored of the row count
        # number of bins is the cutoff length minus 1
        import math
        cutoffs = self.frame.bin_column_equal_width('number')
        self.assertEqual(len(cutoffs) - 1, math.floor(math.sqrt(self.frame.row_count)))




if __name__ == "__main__":
    unittest.main()
