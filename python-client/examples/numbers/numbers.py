#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#



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

import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True

ia.connect()

#loggers.set_http()

print("define csv file")
schema =  [("number", str), ("factor", str), ("binary", str), ("isPrime", str), ("reverse", str), ("isPalindrome", str)]
csv = ia.CsvFile("/numbers.csv", schema, delimiter=":", skip_header_lines=1)

print("create frame")
frame = ia.Frame(csv)

print("inspect frame")
print frame.inspect(10)
print("frame row count " + str(frame.row_count))

print("flatten factor column")
frame.flatten_column("factor")
print frame.inspect(10)
print("frame row count " + str(frame.row_count))

print("define graph parsing rules")
number = ia.VertexRule("number", frame["number"],{ "isPrime": frame["isPrime"], "isPalindrome": frame["isPalindrome"]})
factor = ia.VertexRule("number", frame["factor"])
binary = ia.VertexRule("number", frame["binary"])
reverse = ia.VertexRule("number", frame["reverse"])

hasFactor = ia.EdgeRule("hasFactor", number, factor, bidirectional=False)
hasBinary = ia.EdgeRule("hasBinary", number, binary, bidirectional=False)
hasReverse = ia.EdgeRule("hasReverse", number, reverse, bidirectional=True)

print("create graph")
graph = ia.TitanGraph([number, factor, binary, reverse, hasFactor, hasBinary, hasReverse])
