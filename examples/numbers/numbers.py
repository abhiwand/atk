#
# Numbers example - create a frame and load a graph
#                 - includes flatten_column()
#                 - includes both uni-directional and bi-directional edges
#
# Depends on a numbers.csv file
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal numbers.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/numbers.py')
#

from intelanalytics import *

#loggers.set_http()

print("server ping")
server.ping()

print("define csv file")
schema =  [("number", str), ("factor", str), ("binary", str), ("isPrime", str), ("reverse", str), ("isPalindrome", str)]
csv = CsvFile("/numbers.csv", schema, delimiter=":", skip_header_lines=1)

print("create big frame")
frame = BigFrame(csv)

print("inspect frame")
print frame.inspect(10)
print("frame row count " + str(frame.row_count))

print("flatten factor column")
frame = frame.flatten_column("factor")
print frame.inspect(10)
print("frame row count " + str(frame.row_count))

print("define graph parsing rules")
number = VertexRule("number", frame["number"],{ "isPrime": frame["isPrime"], "isPalindrome": frame["isPalindrome"]})
factor = VertexRule("number", frame["factor"])
binary = VertexRule("number", frame["binary"])
reverse = VertexRule("number", frame["reverse"])

hasFactor = EdgeRule("hasFactor", number, factor, bidirectional=False)
hasBinary = EdgeRule("hasBinary", number, binary, bidirectional=False)
hasReverse = EdgeRule("hasReverse", number, reverse, bidirectional=True)

print("create graph")
graph = BigGraph([number, factor, binary, reverse, hasFactor, hasBinary, hasReverse])
print("created graph " + graph.name)
