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
import trustedanalytics as ia

ia.connect();

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/movie_data_random.csv"

#csv schema definition
schema = [("user_id", ia.int32),
          ("movie_id", ia.int32),
          ("rating", ia.int32),
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

print "Creating Graph recommend"

graph = ia.TitanGraph([user, movie, rates], "recommend")

print "Running Conjugate Gradient Descent on Graph recommend"

print graph.ml.conjugate_gradient_descent(edge_value_property_list=["rating"],
                                          vertex_type_property_key="vertex_type",
                                          input_edge_label_list=["edge"],
                                          output_vertex_property_list=["cgd_result"],
                                          edge_type_property_key="splits",
                                          vector_value=True,
                                          cgd_lambda=0.065,
                                          num_iters=3)

print "Running recommend on Graph recommend"
result = graph.query.gremlin('g.V("vertex_type", "L")[0]')['results'][0]
recommendation = graph.query.recommend(output_vertex_property_list="cgd_result",
                            left_vertex_id_property_key='user_id',
                            right_vertex_id_property_key='movie_id',
                            vertex_type="L",
                            vertex_id=str(result['user_id']),
                            train_str=None,
                            num_output_results=10)

print recommendation['recommend']
