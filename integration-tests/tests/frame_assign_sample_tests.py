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
import math
from intelanalytics.rest.command import CommandServerError

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

_multiprocess_can_split_ = True

class FrameAssignSampleTests(unittest.TestCase):
    """
    Test frame.assign_sample

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        print "define csv file"
        self.schema = [('user', ia.int32),
                         ('vertex_type', str),
                         ('movie', ia.int32),
                         ('rating', ia.int32),
                         ('splits', str)]
        self.csv = ia.CsvFile("/datasets/movie.csv", self.schema)

        print "creating frame"
        self.frame = ia.Frame(self.csv)

    def test_assign_sample_low_probabilities(self):
        try:
            f = self.frame.assign_sample(sample_percentages= [0.1, 0.2], sample_labels=None, output_column='fuBuddy', random_seed=None)
            self.fail("FAIL. Providing probabilities that do not sum to 1 should raise exception from assign_columns")
        except CommandServerError:
            pass

    def test_assign_sample_high_probabilities(self):
        try:
            f = self.frame.assign_sample(sample_percentages= [0.6, 0.5], sample_labels=None, output_column='fuBuddy', random_seed=None)
            self.fail("FAIL. Providing probabilities that do not sum to 1 should raise exception from assign_columns")
        except CommandServerError:
            pass

    def test_assign_sample_column_name(self):
        f = self.frame.assign_sample(sample_percentages= [0.1, 0.2, 0.4, 0.3], sample_labels=None, output_column='fuBuddy', random_seed=None)
        self.assertEqual(f.column_names, [name for name, type in self.schema + ['fuBuddy']])


if __name__ == "__main__":
    unittest.main()
