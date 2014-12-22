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

import unittest
import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

_multiprocess_can_split_ = True

class FrameFlattenColumnTest(unittest.TestCase):
    """
    Test frame.flatten_column()

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

    def test_flatten_column_abc(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_column('abc', delimiter='|')

        # validate
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 16)

    def test_flatten_column_food(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_column('food', delimiter='+')
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 17)

    def test_flatten_column_abc_and_food(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_column('abc', delimiter='|')
        self.frame.flatten_column('food', delimiter='+')

        # validate
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 29)

    def test_flatten_column_does_nothing_with_wrong_delimiter(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_column('abc', delimiter=',')

        # validate
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

if __name__ == "__main__":
    unittest.main()
