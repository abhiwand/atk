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
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class FrameSmokeTest(unittest.TestCase):
    """
        Smoke test basic frame operations (create, add column, delete column, etc).

        This is a build-time test so it needs to be written to be as fast as possible:
            - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
            - Prefer speed over perfect test isolation
            - Add lots of assertions and logging to make up for lack of isolation
            - Tests are ran in Parallel
    """

    def test_create_frame_and_copy(self):
        print "test_frame_1.1 define csv file"
        csv = ia.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', ia.int32),
                                            ('city', str),
                                            ('population_2013', str),
                                            ('pop_2010', str),
                                            ('change', str),
                                            ('county', str)], delimiter='|')

        print "test_frame_1.2 creating frame"
        frame = ia.Frame(csv)
        print
        print frame.inspect(20)
        print
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        self.assertEqual(frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.assertEquals(len(frame.column_names), 6)

        print "test_frame_1.3 get_error_frame()"
        error_frame = frame.get_error_frame()
        print
        print error_frame.inspect(20)
        print
        self.assertEquals(error_frame.row_count, 2, "error frame should have 2 bad rows after loading")
        self.assertEquals(len(error_frame.column_names), 2, "error frames should have 2 columns (original value and error message)")

        print "test_frame_1.4 copy()"
        top10_frame = frame.copy()
        self.assertEquals(top10_frame.row_count, 20, "copy should have same number of rows as original")
        self.assertNotEquals(frame._id, top10_frame._id, "copy should have a different id from the original")

        #print
        #print "0005 test filter() method"
        #top10_frame.filter(lambda row: row.rank < 11)
        #self.assertEquals(top10_frame.row_count, 10, "after filtering there should only be ten cities")
        ##self.assertEqual(top10_frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        #self.assertEquals(frame.row_count, 20, "original frame should not have changed when we modified a copy")

if __name__ == "__main__":
    unittest.main()
