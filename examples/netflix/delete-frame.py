#
# Netflix example - create a frame and delete it
#
# Depends on a netflix.csv file (see sample in data/netflix.csv)
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal netflix.csv {fsRoot in HDFS}
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
csv = CsvFile("/netflix.csv", schema= [('user', int32),
                                              ('vertexType', str),
                                              ('movie', int32),
                                              ('rating', str),
                                              ('splits', str)], skip_header_lines=1)

print("create big frame")
frame = BigFrame(csv)

print("inspect frame")
print frame.inspect(10)

print("delete the frame")
deleted_frame = delete_frame(frame)
print("deleted frame: " + deleted_frame)
