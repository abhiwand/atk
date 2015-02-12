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

print("create big frame")
frame = ia.Frame(csv)

print("inspect frame")
frame.inspect(10)
print("frame row count " + str(frame.row_count))

print("create graph")
graph = ia.Graph()

print("created graph " + graph.name)

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
