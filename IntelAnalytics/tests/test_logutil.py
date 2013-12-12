##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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
from intel_analytics.report import find_progress


class TestLogUtil(unittest.TestCase):
    def test_invalid_line(self):
        progress = find_progress("13/11/14 14:35:52 INFO mapred.JobClient: Running job: job_201311121330_0046")
        self.assertEqual(progress, None)

    def test_empty_line(self):
        progress = find_progress("")
        self.assertEquals(progress, None)

    def test_valid_line_1(self):
        progress = find_progress("13/11/14 14:36:05 INFO mapred.JobClient:  map 100% reduce 33%")
        self.assertEquals(100, progress.mapper_progress)
        self.assertEquals(33, progress.reducer_progress)

    def test_valid_line_2(self):
        progress = find_progress("13/11/14 14:36:05 INFO mapred.JobClient:  map 0% reduce 0%")
        self.assertEquals(0, progress.mapper_progress)
        self.assertEquals(0, progress.reducer_progress)

    def test_reading_from_file(self):
        currentProgress = None
        with open("../tests/MapReduceLogSample", "r") as logFile:
            for line in logFile:
                progress = find_progress(line)
                if(progress != None):
                    currentProgress = progress
                    #print("map:" + str(progress.getMapperProgress()) + ", reduce:" + str(progress.getReducerProgress()))

        self.assertEquals(100, currentProgress.mapper_progress)
        self.assertEquals(100, currentProgress.reducer_progress)





if __name__ == '__main__':
    unittest.main()
