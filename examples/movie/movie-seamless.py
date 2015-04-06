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
ia.loggers.set_api()

ia.connect()

# loggers.set_http()

print("server ping")
ia.server.ping()

print("define csv file")
csv = ia.CsvFile("/movie.csv", schema= [('user', ia.int32),
                                              ('vertexType', str),
                                              ('movie', ia.int32),
                                              ('rating', ia.int32),
                                              ('splits', str)])

print("create frame")
frame = ia.Frame(csv)

print("inspect frame")
frame.inspect(10)
print("frame row count " + str(frame.row_count))

print("create graph")
graph = ia.Graph()

print("created graph ")

print("define vertices and edges")
graph.define_vertex_type('movies')
graph.define_vertex_type('users')
graph.define_edge_type('ratings', 'users', 'movies', directed=False)

print "add user vertices"
graph.vertices['users'].add_vertices( frame, 'user')
# graph.vertex_count

graph.vertices['users'].inspect(20)

print "add movie vertices"
graph.vertices['movies'].add_vertices( frame, 'movie')
# graph.vertex_count
graph.edges['ratings'].add_edges(frame, 'user', 'movie', ['rating'], create_missing_vertices=False)

print ("vertex count: " + str(graph.vertex_count))
print ("edge count: " + str(graph.edge_count))

graph.edges['ratings'].inspect(20)

print "done"
