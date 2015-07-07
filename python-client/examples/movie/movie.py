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

import taprootanalytics as ia

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

print("create frame")
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
