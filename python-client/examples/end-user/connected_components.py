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


def run(path=r"datasets/movie_data_random.csv"):
    """
    The default home directory is hdfs://user/taproot all the sample data sets are saved to
    hdfs://user/taproot/datasets when installing through the rpm
    you will need to copy the data sets to hdfs manually otherwise and adjust the data set location path accordingly
    :param path: data set hdfs path can be full and relative path
    """
    import taprootanalytics as ta

    ta.connect()

    #csv schema definition
    schema = [("user_id", ta.int64),
              ("movie_id", ta.int64),
              ("rating", ta.int64),
              ("splits", str)]

    csv_file = ta.CsvFile(path, schema, skip_header_lines=1)

    print "Building data frame"

    frame = ta.Frame(csv_file)

    print "Done building data frame"

    print "Inspecting frame"

    print frame.inspect()

    user = ta.VertexRule("user_id", frame.user_id, {"vertex_type": "L"})

    movie = ta.VertexRule("movie_id", frame.movie_id, {"vertex_type": "R"})

    rates = ta.EdgeRule("edge", user, movie, {"splits": frame.splits, "rating": frame.rating}, bidirectional=True)

    print "Creating Graph connected_components_demo"

    graph = ta.TitanGraph([user, movie, rates], "connected_components_demo")

    print "Running Connected Components Algorithm"

    print graph.ml.connected_components(input_edge_label="edge",
                                        output_vertex_property="component_id")
