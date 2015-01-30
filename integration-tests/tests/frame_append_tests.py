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

_multiprocess_can_split_ = True

class FrameAppendTests(unittest.TestCase):
    """
    Test frame.append

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    def setUp(self):
        self.schema1 = [('rank', ia.int32),
                        ('city', str),
                        ('population_2013', str),
                        ('pop_2010', str),
                        ('change', str),
                        ('county', str)]
        self.schema2 = [('number', ia.int32),
                        ('abc', str),
                        ('food', str)]
        self.combined_schema = []
        self.combined_schema.extend(self.schema1)
        self.combined_schema.extend(self.schema2)

        self.csv1 = ia.CsvFile("/datasets/oregon-cities.csv", schema=self.schema1, delimiter='|')
        self.csv2 = ia.CsvFile("/datasets/flattenable.csv", schema= self.schema2, delimiter=',')

    def test_append_to_empty_frame(self):
        frame = ia.Frame()
        self.assertEqual(frame.row_count, 0)
        self.assertEqual(frame.column_names, [])

        frame.append(self.csv1)
        self.assertEqual(frame.row_count, 20)
        self.assertEqual(frame.column_names, [name for name, type in self.schema1])

    def test_append_same_schema(self):
        frame = ia.Frame(self.csv1)
        self.assertEqual(frame.row_count, 20)
        self.assertEqual(frame.column_names, [name for name, type in self.schema1])
        frame.append(self.csv1)
        self.assertEqual(frame.row_count, 40)
        self.assertEqual(frame.column_names, [name for name, type in self.schema1])

    def test_append_new_columns(self):
        frame = ia.Frame(self.csv1)
        self.assertEqual(frame.row_count, 20)
        self.assertEqual(frame.column_names, [name for name, type in self.schema1])
        frame.append(self.csv2)
        self.assertEqual(frame.row_count, 30)
        self.assertEqual(frame.column_names, [name for name, type in self.combined_schema])

    def test_append_frame(self):
        src_frame = ia.Frame(self.csv1)
        self.assertEqual(src_frame.row_count, 20)
        self.assertEqual(src_frame.column_names, [name for name, type in self.schema1])

        dest_frame = ia.Frame(self.csv2)
        self.assertEqual(dest_frame.row_count, 10)
        self.assertEqual(dest_frame.column_names,[name for name, type in  self.schema2])

        src_frame.append(dest_frame)
        self.assertEqual(src_frame.row_count, 30)
        self.assertEqual(src_frame.column_names, [name for name, type in self.combined_schema])




if __name__ == "__main__":
    unittest.main()
