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
import sys
import iatest
from intelanalytics.core.userfunction import has_python_user_function_arg

iatest.init()

import unittest

sample_worker_message = """Job aborted due to stage failure: Task 0.0:87 failed 4 times, most recent failure: Exception failure in TID 181 on host gao-wsb.jf.intel.com: org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File \"/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/spark/python/pyspark/worker.py\", line 77, in main
    serializer.dump_stream(func(split_index, iterator), outfile)
  File \"/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/spark/python/intelanalytics/rest/spark.py\", line 138, in dump_stream
    self.dump_stream_as_json(self._batched(iterator), stream)
  File \"/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/spark/python/intelanalytics/rest/spark.py\", line 141, in dump_stream_as_json
    for obj in iterator:
  File \"/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/spark/python/intelanalytics/rest/serializers.py\", line 180, in _batched
    for item in iterator:
  File \"intelanalytics/rest/spark.py\", line 106, in row_func
    return row_function(row_wrapper)
  File \"intelanalytics/rest/spark.py\", line 51, in add_one_column
    result = row_function(row)
  File \"<ipython-input-8-e6a5608355ef>\", line 1, in <lambda>
ZeroDivisionError: integer division or modulo by zero

        org.apache.spark.api.python.PythonRDD$$anon$1.read(PythonRDD.scala:118)
        org.apache.spark.api.python.PythonRDD$$anon$1.<init>(PythonRDD.scala:148)
        org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:81)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.MappedRDD.compute(MappedRDD.scala:31)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.FlatMappedRDD.compute(FlatMappedRDD.scala:33)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.MappedRDD.compute(MappedRDD.scala:31)
        com.intel.intelanalytics.engine.spark.frame.FrameRDD.compute(FrameRDD.scala:15)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:35)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.MappedRDD.compute(MappedRDD.scala:31)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:111)
        org.apache.spark.scheduler.Task.run(Task.scala:51)
        org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:187)
        java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
        java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
        java.lang.Thread.run(Thread.java:724)
Driver stacktrace:
"""

class TestUserFunc(unittest.TestCase):


    def test_trim_spark_worker_trace_from_exception(self):
        e = Exception(sample_worker_message)
        filter = "        org.apache.spark.api.python"
        self.assertTrue(e.args[0].find(filter) >= 0)

        @has_python_user_function_arg
        def func():
            raise e

        try:
            func()
        except Exception as ex:
            # verify that the spark worker stacktrace is removed from the message
            self.assertTrue(ex.args[0].find(filter) < 0)
        else:
            self.fail()







