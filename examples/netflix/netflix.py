#
# Netflix example - create a frame and load a graph
#
# Depends on a netflix.csv file
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal netflix.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/netflix.py')
#

from intelanalytics import *

#loggers.set_http()

print("server ping")
server.ping()

print("define csv file")
csv = CsvFile("/netflix.csv", schema= [('user', int32),
                                              ('vertexType', str),
                                              ('movie', int32),
                                              ('rating', str),
                                              ('splits', str)])

print("create big frame")
frame = BigFrame(csv)

errors = frame.get_error_frame()

print("inspect frame")
print frame.inspect(10)

print("inspect frame errors")
print errors.inspect(10)

print("define graph parsing rules")
movie = VertexRule("movie", frame.movie)
user = VertexRule("user", frame.user, {"vertexType": frame.vertexType})
rates = EdgeRule("rating", user, movie, { "splits": frame.splits }, is_directed = True)

print("create graph")
graph = BigGraph([user, movie, rates])
print("created graph " + graph.name)