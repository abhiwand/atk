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

csv = ia.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', ia.int32),
                                                         ('city', str),
                                                         ('population_2013', str),
                                                         ('pop_2010', str),
                                                         ('change', str),
                                                         ('county', str)], delimiter='|')


class FrameUdfTests(unittest.TestCase):
    """
    Tests APIs that use Python UDFs
    """

    _multiprocess_can_split_ = True

    def test_filter(self):
        frame = ia.Frame(csv)
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        frame.filter(lambda row: row.county == "Washington")
        self.assertEquals(frame.row_count, 4, "frame should have 4 rows after filtering")
        cities = frame.take(frame.row_count, columns="city")
        self.assertEquals(sorted(map(lambda f: str(f[0]), cities)), ["Beaverton", "Hillsboro", "Tigard","Tualatin"])

    def test_add_columns_and_copy_where(self):
        """
        Tests UDFs for add_columns and copy(where), and uses the vector type

        Changes the 2 population strings to a vector, and then uses the vector
        to compute the change, and then copy out all the incorrect ones
        """
        frame = ia.Frame(csv)
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        frame.add_columns(lambda row: [float(row['pop_2010'].translate({ord(','): None})),
                                       float(row['population_2013'].translate({ord(','): None}))],
                          ("vpops", ia.vector(2)))
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        self.assertEquals(frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county', 'vpops'])
        frame.add_columns(lambda row: (row.vpops[1] - row.vpops[0])/row.vpops[0], ("comp_change", ia.float64))
        #print frame.inspect(20)
        bad_cities = frame.copy(columns=['city', 'change', 'comp_change'], where=lambda row: row.change != "%.2f%%" % round(100*row.comp_change, 2))
        self.assertEquals(bad_cities.column_names, ['city', 'change', 'comp_change'])
        self.assertEquals(bad_cities.row_count, 1)
        #print bad_cities.inspect()
        row = bad_cities.take(1)[0]
        row[2] = round(row[2], 5)
        self.assertEquals(row, [u'Tualatin', u'4.17%', 0.03167])  # should just be one bad one, Tualatin


if __name__ == "__main__":
    unittest.main()
