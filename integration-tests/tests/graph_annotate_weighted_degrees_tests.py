# ##############################################################################
# # INTEL CONFIDENTIAL
# #
# # Copyright 2015 Intel Corporation All Rights Reserved.
# #
# # The source code contained or described herein and all documents related to
# # the source code (Material) are owned by Intel Corporation or its suppliers
# # or licensors. Title to the Material remains with Intel Corporation or its
# # suppliers and licensors. The Material may contain trade secrets and
# # proprietary and confidential information of Intel Corporation and its
# # suppliers and licensors, and is protected by worldwide copyright and trade
# # secret laws and treaty provisions. No part of the Material may be used,
# # copied, reproduced, modified, published, uploaded, posted, transmitted,
# # distributed, or disclosed in any way without Intel's prior express written
# # permission.
# #
# # No license under any patent, copyright, trade secret or other intellectual
# # property right is granted to or conferred upon you by disclosure or
# # delivery of the Materials, either expressly, by implication, inducement,
# # estoppel or otherwise. Any license under such intellectual property rights
# # must be express and approved by Intel in writing.
# ##############################################################################
#
# import unittest
# import intelanalytics as ia
#
# # show full stack traces
# ia.errors.show_details = True
# ia.loggers.set_api()
# # TODO: port setup should move to a super class
# if ia.server.port != 19099:
#     ia.server.port = 19099
# ia.connect()
#
# class GraphAnnotateWeightedDegreesTest(unittest.TestCase):
#     def testAnnotateWeightedDegrees(self):
#         print "define csv file"
#         schema_node = [("nodename", str),
#                        ("in", ia.int64),
#                        ("out", ia.int64),
#                        ("undirectedcount", ia.int64),
#                        ("isundirected", ia.int64),
#                        ("outlabel", ia.int64),
#                        ("insum", ia.float64),
#                        ("outsum", ia.float64),
#                        ("undirectedsum", ia.float64),
#                        ("labelsum", ia.float64),
#                        ("nolabelsum", ia.float64),
#                        ("defaultsum", ia.float64),
#                        ("integersum", ia.int64)]
#
#         schema_directed = [("nodefrom", str),
#                            ("nodeto", str),
#                            ("value", ia.float64),
#                            ("badvalue", str),
#                            ("intvalue", ia.int32),
#                            ("int64value", ia.int64)]
#
#         schema_undirected = [("node1", str),
#                              ("node2", str),
#                              ("value", ia.float64)]
#
#         schema_directed_label = [("nodefrom", str),
#                                  ("nodeto", str),
#                                  ("labeltest", ia.float64)]
#
#         node_frame = ia.Frame(ia.CsvFile("/datasets/annotate_node_list.csv",schema_node))
#         directed_frame = ia.Frame(ia.CsvFile("/datasets/annotate_directed_list.csv",schema_directed))
#         undirected_frame = ia.Frame(ia.CsvFile("/datasets/annotate_undirected_list.csv", schema_undirected))
#         directed_label_frame = ia.Frame(ia.CsvFile("/datasets/annotate_directed_label_list.csv", schema_directed_label))
#
#         graph = ia.Graph()
#         graph.define_vertex_type("primary")
#         graph.vertices['primary'].add_vertices(node_frame,"nodename",["out",
#                                                                       "undirectedcount",
#                                                                       "isundirected",
#                                                                       "outlabel",
#                                                                       "in",
#                                                                       "insum",
#                                                                       "outsum",
#                                                                       "undirectedsum",
#                                                                       "labelsum",
#                                                                       "nolabelsum",
#                                                                       "defaultsum",
#                                                                       "integersum"])
#         graph.define_edge_type("directed","primary","primary",directed=True)
#         graph.define_edge_type("labeldirected", "primary", "primary",
#                                directed=True)
#         graph.define_edge_type("undirected", "primary", "primary",
#                                directed=False)
#
#         graph.edges['directed'].add_edges(directed_frame, "nodefrom",
#                                           "nodeto", ["value",
#                                                      "badvalue",
#                                                      "intvalue",
#                                                      "int64value"])
#         graph.edges['labeldirected'].add_edges(directed_label_frame,
#                                                "nodefrom", "nodeto",
#                                                ["labeltest"])
#         graph.edges['undirected'].add_edges(undirected_frame, "node1",
#                                             "node2", ["value"])
#         output = graph.annotate_weighted_degrees("sumName", degree_option="in",edge_weight_property="value")
#         self.assertTrue(type(output) is dict)
#         self.assertTrue(output.has_key('primary'))
#         frame_parquet = output['primary']
#         self.assertTrue(dict(frame_parquet.schema).has_key('sumName'))