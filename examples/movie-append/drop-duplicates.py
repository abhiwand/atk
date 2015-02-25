//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

#
# Netflix example where we append two frames together and then drop duplicates
#
# Depends on a netflix1.csv and netflix2.csv files that each contain some overlapping data and some unique data.
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal netflix*.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/netflix-frame-append.py')
#

from intelanalytics import *

#loggers.set_http()

print("server ping")
server.ping()

print("define csv file")
schema = [('user', int32),('vertexType', str),('movie', int32),('rating', str),('splits', str)]

csv1 = CsvFile("/netflix1.csv", schema)
csv2 = CsvFile("/netflix2.csv", schema)

print("create big frame 1")
frame1 = BigFrame(csv1)
print("create big frame 2")
frame2 = BigFrame(csv2)

print("inspect 1st frame")
print frame1.inspect(10)

print("inspect 2nd frame")
print frame2.inspect(10)

print("frame1 row count " + str(frame1.row_count))
print("frame2 row count " + str(frame2.row_count))

print("append frame2 to frame1")
frame1.append(frame2)

print("frame1 row count " + str(frame1.row_count))
print("frame2 row count " + str(frame2.row_count))

print("drop duplicates on frame1")
frame1.drop_duplicates()
print("frame1 row count " + str(frame1.row_count))

print("drop duplicates second time should have same output")
frame1.drop_duplicates()
print("frame1 row count " + str(frame1.row_count))

print("copy frame1 to frame3")
frame3 = frame1.copy()
print("frame3 row count " + str(frame3.row_count))

print("append frame1 to frame3")
frame3.append(frame1)
print("frame3 row count " + str(frame3.row_count))

print("append frame2 to frame3")
frame3.append(frame2)
print("frame3 row count " + str(frame3.row_count))

print("drop duplicates on frame3")
frame3.drop_duplicates()
print("frame3 row count " + str(frame3.row_count))
