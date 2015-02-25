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

#
# Movie example where we import multiple files using wildcards
#
# Depends on a movie1.csv and movie2.csv files
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal movie*.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/frame-wildcards.py')
#

from intelanalytics import *

#loggers.set_http()

print("server ping")
server.ping()

print("define csv file")
schema = [('user', int32),('vertexType', str),('movie', int32),('rating', str),('splits', str)]

csv = CsvFile("/movie*.csv", schema)

print("create big frame")
frame = BigFrame(csv)

#print("inspect frame")
#print frame.inspect(10)

print("frame row count " + str(frame.row_count))

print "drop duplicates"
frame.drop_duplicates()

print("frame row count " + str(frame.row_count))
