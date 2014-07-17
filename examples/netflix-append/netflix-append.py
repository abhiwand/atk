#
# Netflix example where we append data to an existing graph
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
#       execfile('/path/to/netflix-append.py')
#

from intelanalytics import *

loggers.set_http()

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

print("create graph")
movie1 = VertexRule("movie", frame1.movie)
user1 = VertexRule("user", frame1.user, {"vertexType": frame1.vertexType})
rates1 = EdgeRule("rating", user1, movie1, { "splits": frame1.splits }, is_directed = True)
graph = BigGraph([user1, movie1, rates1])

print("append more data to the graph")
movie2 = VertexRule("movie", frame2.movie)
user2 = VertexRule("user", frame2.user, {"vertexType": frame2.vertexType})
rates2 = EdgeRule("rating", user2, movie2, { "splits": frame2.splits }, is_directed = True)
graph.append([user2, movie2, rates2])

print("done")
