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

class FrameDropColumnsTest(unittest.TestCase):
    """
    Test frame.drop_columns()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """

    def setUp(self):
        print "define csv file"
        csv = ia.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', ia.int32),
                                                             ('city', str),
                                                             ('population_2013', str),
                                                             ('pop_2010', str),
                                                             ('change', str),
                                                             ('county', str)], delimiter='|')

        print "create frame"
        self.frame = ia.Frame(csv)

    def test_drop_first_column(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns("rank")
        self.assertEqual(self.frame.column_names, ['city', 'population_2013', 'pop_2010', 'change', 'county'])

    def test_drop_second_column(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns("city")
        self.assertEqual(self.frame.column_names, ['rank', 'population_2013', 'pop_2010', 'change', 'county'])

    def test_drop_last_column(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns("county")
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change'])

    def test_drop_multiple_columns_001(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns(['rank', 'pop_2010'])
        self.assertEqual(self.frame.column_names, ['city', 'population_2013', 'change', 'county'])

    def test_drop_multiple_columns_002(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns(['city', 'rank', 'pop_2010', 'change'])
        self.assertEqual(self.frame.column_names, ['population_2013', 'county'])

if __name__ == "__main__":
    unittest.main()