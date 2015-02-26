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
# Movie example where we append data to an existing graph
#
# Depends on a movie1.csv and movie2.csv files that each contain some overlapping data and some unique data.
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal movie*.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/movie-graph-append.py')
#

from intelanalytics import *

#loggers.set_http()

print("server ping")
server.ping()

print("define csv file")
schema = [('user', int32),('vertexType', str),('movie', int32),('rating', str),('splits', str)]

csv1 = CsvFile("/movie1.csv", schema)
csv2 = CsvFile("/movie2.csv", schema)

print("create big frame 1")
frame1 = BigFrame(csv1)
print("create big frame 2")
frame2 = BigFrame(csv2)

print("inspect 1st frame")
print frame1.inspect(10)

print("inspect 2nd frame")
print frame2.inspect(10)

print("create graph")
movie1 = VertexRule("movie", frame1.movie)
user1 = VertexRule("user", frame1.user, {"vertexType": frame1.vertexType})
rates1 = EdgeRule("rating", user1, movie1, { "splits": frame1.splits }, bidirectional = False)
graph = BigGraph([user1, movie1, rates1])

print("append more data to the graph")
movie2 = VertexRule("movie", frame2.movie)
user2 = VertexRule("user", frame2.user, {"vertexType": frame2.vertexType})
rates2 = EdgeRule("rating", user2, movie2, { "splits": frame2.splits }, bidirectional = False)
graph.append([user2, movie2, rates2])

print("grame name: " + graph.name)
print("done")
