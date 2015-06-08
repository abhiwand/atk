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

ia.connect();

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/lbp_edge.csv"

#csv schema definition
schema = [("source", ia.int64),
          ("value", str),
          ("vertex_type", str),
          ("target", ia.int64),
          ("weight", ia.float64)]

csv = ia.CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame 'myframe'"

frame = ia.Frame(csv, "myframe")

print "Done building data frame 'myframe'"

print "Inspecting frame 'myframe'"

print frame.inspect()

source = ia.VertexRule("source", frame.source, {"vertex_type": frame.vertex_type, "value": frame.value})

target = ia.VertexRule("target", frame.target, {"vertex_type": frame.vertex_type, "value": frame.value})

edge = ia.EdgeRule("edge", target, source, {'weight': frame.weight}, bidirectional=True)

print "Creating Graph 'mygraph'"

graph = ia.TitanGraph([target, source, edge], "mygraph")

print "Running Loopy Belief Propagation on Graph mygraph"

print graph.ml.loopy_belief_propagation(vertex_value_property_list=["value"],
                                        edge_value_property_list=["weight"],
                                        input_edge_label_list=["edge"],
                                        output_vertex_property_list=["lbp_posterior"],
                                        vertex_type="vertex_type",
                                        vector_value=True,
                                        max_supersteps=10,
                                        convergence_threshold=0.0,
                                        anchor_threshold=0.9,
                                        smoothing=2.0,
                                        ignore_vertex_type=False,
                                        max_product=False,
                                        power=0)
