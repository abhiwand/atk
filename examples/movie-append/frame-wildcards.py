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
