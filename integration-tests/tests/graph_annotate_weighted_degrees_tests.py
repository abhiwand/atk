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

import unittest
import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class GraphAnnotateWeightedDegreesTest(unittest.TestCase):
    def test_annotate_weighted_degrees(self):
        print "define csv file"
        schema_node = [("nodename", str),
                       ("in", ia.int64),
                       ("out", ia.int64),
                       ("undirectedcount", ia.int64),
                       ("isundirected", ia.int64),
                       ("outlabel", ia.int64),
                       ("insum", ia.float64),
                       ("outsum", ia.float64),
                       ("undirectedsum", ia.float64),
                       ("labelsum", ia.float64),
                       ("nolabelsum", ia.float64),
                       ("defaultsum", ia.float64),
                       ("integersum", ia.int64)]

        schema_directed = [("nodefrom", str),
                           ("nodeto", str),
                           ("value", ia.float64),
                           ("badvalue", str),
                           ("intvalue", ia.int32),
                           ("int64value", ia.int64)]

        schema_undirected = [("node1", str),
                             ("node2", str),
                             ("value", ia.float64)]

        schema_directed_label = [("nodefrom", str),
                                 ("nodeto", str),
                                 ("labeltest", ia.float64)]

        node_frame = ia.Frame(ia.CsvFile("/datasets/annotate_node_list.csv",schema_node))
        directed_frame = ia.Frame(ia.CsvFile("/datasets/annotate_directed_list.csv",schema_directed))
        undirected_frame = ia.Frame(ia.CsvFile("/datasets/annotate_undirected_list.csv", schema_undirected))
        directed_label_frame = ia.Frame(ia.CsvFile("/datasets/annotate_directed_label_list.csv", schema_directed_label))

        graph = ia.Graph()
        graph.define_vertex_type("primary")
        graph.vertices['primary'].add_vertices(node_frame,"nodename",["out",
                                                                      "undirectedcount",
                                                                      "isundirected",
                                                                      "outlabel",
                                                                      "in",
                                                                      "insum",
                                                                      "outsum",
                                                                      "undirectedsum",
                                                                      "labelsum",
                                                                      "nolabelsum",
                                                                      "defaultsum",
                                                                      "integersum"])
        graph.define_edge_type("directed","primary","primary",directed=True)
        graph.define_edge_type("labeldirected", "primary", "primary",
                               directed=True)
        graph.define_edge_type("undirected", "primary", "primary",
                               directed=False)

        graph.edges['directed'].add_edges(directed_frame, "nodefrom",
                                          "nodeto", ["value",
                                                     "badvalue",
                                                     "intvalue",
                                                     "int64value"])
        graph.edges['labeldirected'].add_edges(directed_label_frame,
                                               "nodefrom", "nodeto",
                                               ["labeltest"])
        graph.edges['undirected'].add_edges(undirected_frame, "node1",
                                            "node2", ["value"])
        output = graph.annotate_weighted_degrees("sumName", degree_option="in",edge_weight_property="value")
        self.assertTrue(type(output) is dict)
        self.assertTrue(output.has_key('primary'))
        frame_parquet = output['primary']
        self.assertTrue(dict(frame_parquet.schema).has_key('sumName'))

if __name__ == "__main__":
    unittest.main()