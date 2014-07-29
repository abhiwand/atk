#
# Netflix example where we append two frames together and then drop duplicates
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
#       execfile('/path/to/netflix-frame-append.py')
#

from intelanalytics import *

#loggers.set_http()

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

print("frame1 row count " + str(frame1.row_count))
print("frame2 row count " + str(frame2.row_count))

print("append frame2 to frame1")
frame1.append(frame2)

print("frame1 row count " + str(frame1.row_count))
print("frame2 row count " + str(frame2.row_count))

print("drop duplicates on frame1")
frame1.drop_duplicates()
print("frame1 row count " + str(frame1.row_count))

print("drop duplicates second time should have same output")
frame1.drop_duplicates()
print("frame1 row count " + str(frame1.row_count))

print("copy frame1 to frame3")
frame3 = frame1.copy()
print("frame3 row count " + str(frame3.row_count))

print("append frame1 to frame3")
frame3.append(frame1)
print("frame3 row count " + str(frame3.row_count))

print("append frame2 to frame3")
frame3.append(frame2)
print("frame3 row count " + str(frame3.row_count))

print("drop duplicates on frame3")
frame3.drop_duplicates()
print("frame3 row count " + str(frame3.row_count))