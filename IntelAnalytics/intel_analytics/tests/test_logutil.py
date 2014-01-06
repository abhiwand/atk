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
import unittest, os
from intel_analytics.report import find_progress


class TestLogUtil(unittest.TestCase):
    MAP_REDUCE_LOG = """13/11/14 14:35:51 WARN mapred.JobClient: Use GenericOptionsParser for parsing the arguments. Applications should implement Tool for the same.
13/11/14 14:35:52 INFO input.FileInputFormat: Total input paths to process : 3
13/11/14 14:35:52 INFO util.NativeCodeLoader: Loaded the native-hadoop library
13/11/14 14:35:52 WARN snappy.LoadSnappy: Snappy native library not loaded
13/11/14 14:35:52 INFO mapred.JobClient: Running job: job_201311121330_0046
13/11/14 14:35:53 INFO mapred.JobClient:  map 0% reduce 0%
13/11/14 14:35:58 INFO mapred.JobClient:  map 66% reduce 0%
13/11/14 14:36:00 INFO mapred.JobClient:  map 100% reduce 0%
13/11/14 14:36:05 INFO mapred.JobClient:  map 100% reduce 33%
13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%
13/11/14 14:36:08 INFO mapred.JobClient: Job complete: job_201311121330_0046
13/11/14 14:36:08 INFO mapred.JobClient: Counters: 29
13/11/14 14:36:08 INFO mapred.JobClient:   Job Counters
13/11/14 14:36:08 INFO mapred.JobClient:     Launched reduce tasks=1
13/11/14 14:36:08 INFO mapred.JobClient:     SLOTS_MILLIS_MAPS=9733
13/11/14 14:36:08 INFO mapred.JobClient:     Total time spent by all reduces waiting after reserving slots (ms)=0
13/11/14 14:36:08 INFO mapred.JobClient:     Total time spent by all maps waiting after reserving slots (ms)=0
13/11/14 14:36:08 INFO mapred.JobClient:     Launched map tasks=3
13/11/14 14:36:08 INFO mapred.JobClient:     Data-local map tasks=3
13/11/14 14:36:08 INFO mapred.JobClient:     SLOTS_MILLIS_REDUCES=9619
13/11/14 14:36:08 INFO mapred.JobClient:   File Output Format Counters
13/11/14 14:36:08 INFO mapred.JobClient:     Bytes Written=24202722
13/11/14 14:36:08 INFO mapred.JobClient:   FileSystemCounters
13/11/14 14:36:08 INFO mapred.JobClient:     FILE_BYTES_READ=34200641
13/11/14 14:36:08 INFO mapred.JobClient:     HDFS_BYTES_READ=16281922
13/11/14 14:36:08 INFO mapred.JobClient:     FILE_BYTES_WRITTEN=65481243
13/11/14 14:36:08 INFO mapred.JobClient:     HDFS_BYTES_WRITTEN=24202722
13/11/14 14:36:08 INFO mapred.JobClient:   File Input Format Counters
13/11/14 14:36:08 INFO mapred.JobClient:     Bytes Read=16281550
13/11/14 14:36:08 INFO mapred.JobClient:   Map-Reduce Framework
13/11/14 14:36:08 INFO mapred.JobClient:     Map output materialized bytes=31062806
13/11/14 14:36:08 INFO mapred.JobClient:     Map input records=155471
13/11/14 14:36:08 INFO mapred.JobClient:     Reduce shuffle bytes=31062806
13/11/14 14:36:08 INFO mapred.JobClient:     Spilled Records=1819346
13/11/14 14:36:08 INFO mapred.JobClient:     Map output bytes=29433477
13/11/14 14:36:08 INFO mapred.JobClient:     Total committed heap usage (bytes)=803930112
13/11/14 14:36:08 INFO mapred.JobClient:     CPU time spent (ms)=10800
13/11/14 14:36:08 INFO mapred.JobClient:     Combine input records=0
13/11/14 14:36:08 INFO mapred.JobClient:     SPLIT_RAW_BYTES=372
13/11/14 14:36:08 INFO mapred.JobClient:     Reduce input records=775685
13/11/14 14:36:08 INFO mapred.JobClient:     Reduce input groups=311454
13/11/14 14:36:08 INFO mapred.JobClient:     Combine output records=0
13/11/14 14:36:08 INFO mapred.JobClient:     Physical memory (bytes) snapshot=909611008
13/11/14 14:36:08 INFO mapred.JobClient:     Reduce output records=311454
13/11/14 14:36:08 INFO mapred.JobClient:     Virtual memory (bytes) snapshot=3800096768
13/11/14 14:36:08 INFO mapred.JobClient:     Map output records=775685"""

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
        logFile = self.MAP_REDUCE_LOG.split(os.linesep)
        for line in logFile:
            progress = find_progress(line)
            if(progress != None):
                currentProgress = progress
                #print("map:" + str(progress.getMapperProgress()) + ", reduce:" + str(progress.getReducerProgress()))

        self.assertEquals(100, currentProgress.mapper_progress)
        self.assertEquals(100, currentProgress.reducer_progress)





if __name__ == '__main__':
    unittest.main()
