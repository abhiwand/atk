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

class FrameCopyTest(unittest.TestCase):
    """
    Test copy()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """

    def test_copy_001(self):
        print "define csv file"
        csv = ia.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', ia.int32),
                                            ('city', str),
                                            ('population_2013', str),
                                            ('pop_2010', str),
                                            ('change', str),
                                            ('county', str)], delimiter='|')

        print "create frame"
        frame = ia.Frame(csv)

        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        self.assertEqual(frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])

        print "copy()"
        top10_frame = frame.copy()
        self.assertEquals(top10_frame.row_count, 20, "copy should have same number of rows as original")
        self.assertNotEquals(frame._id, top10_frame._id, "copy should have a different id from the original")

        # TODO: add verification that one Python UDF is working (not working yet)

        #print "filter()"
        #top10_frame.filter(lambda row: row.rank < 11)
        #self.assertEquals(top10_frame.row_count, 10, "after filtering there should only be ten cities")
        ##self.assertEqual(top10_frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        #self.assertEquals(frame.row_count, 20, "original frame should not have changed when we modified a copy")

if __name__ == "__main__":
    unittest.main()
