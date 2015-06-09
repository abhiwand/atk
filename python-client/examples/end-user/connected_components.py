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



#!/usr/bin/python2.7
import intelanalytics as ia

ia.connect()

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/movie_data_random.csv"

#csv schema definition
schema = [("user_id", ia.int64),
          ("movie_id", ia.int64),
          ("rating", ia.int64),
          ("splits", str)]

csv_file = ia.CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame"

frame = ia.Frame(csv_file)

print "Done building data frame"

print "Inspecting frame"

print frame.inspect()

user = ia.VertexRule("user_id", frame.user_id, {"vertex_type": "L"})

movie = ia.VertexRule("movie_id", frame.movie_id, {"vertex_type": "R"})

rates = ia.EdgeRule("edge", user, movie, {"splits": frame.splits, "rating": frame.rating}, bidirectional=True)

print "Creating Graph connected_components_demo"

graph = ia.TitanGraph([user, movie, rates], "connected_components_demo")

print "Running Connected Components Algorithm"

print graph.ml.connected_components(input_edge_label="edge",
                                    output_vertex_property="component_id")
