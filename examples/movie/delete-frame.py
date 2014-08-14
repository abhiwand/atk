#
# Movie example - create a frame and delete it
#
# Depends on a movie.csv file (see sample in data/movie.csv)
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal movie.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/delete-frame.py')
#

from intelanalytics import *

#loggers.set_http()

print("server ping")
server.ping()

print("define csv file")
csv = CsvFile("/movie.csv", schema= [('user', int32),
                                              ('vertexType', str),
                                              ('movie', int32),
                                              ('rating', str),
                                              ('splits', str)], skip_header_lines=1)

print("create big frames")
frame1 = BigFrame(csv)
frame2 = BigFrame(csv, "myframe")

print("inspect frame 1")
print frame1.inspect(10)

print("delete frame1 passing BigFrame")
print delete_frame(frame1)

print("delete frame2 passing the name")
print delete_frame("myframe")
