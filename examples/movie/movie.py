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