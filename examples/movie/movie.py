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
# Movie example - create a frame and load a graph
#
# Depends on a movie.csv file
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal movie.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/movie.py')
#

import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True

ia.connect()

#loggers.set_http()

print("server ping")
ia.server.ping()

print("define csv file")
csv = ia.CsvFile("/movie.csv", schema= [('user', ia.int32),
                                        ('vertexType', str),
                                        ('movie', ia.int32),
                                        ('rating', str),
                                        ('splits', str)])

print("create big frame")
frame = ia.Frame(csv)



errors = frame.get_error_frame()

print("inspect frame")
print frame.inspect(10)
print("frame row count " + str(frame.row_count))

print("inspect frame errors")
print errors.inspect(10)
print("frame row count " + str(errors.row_count))

print("define graph parsing rules")
movie = ia.VertexRule("movie", frame.movie)
user = ia.VertexRule("user", frame.user, {"vertexType": frame.vertexType})
rates = ia.EdgeRule("rating", user, movie, { "splits": frame.splits }, bidirectional = False)

print("create graph")
graph = ia.TitanGraph([user, movie, rates])
print("created graph " + graph.name)
