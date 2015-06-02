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
# class GraphTriangleCountTest(unittest.TestCase):
#     def testTriangleCount(self):
#         graph_data = "/datasets/triangle_count_small.csv"
#         schema = [('from_node',str),('to_node',str),('max_k',ia.int64),('cc',ia.int64)]
#         frame = ia.Frame(ia.CsvFile(graph_data,schema))
#         graph= ia.Graph()
#         graph.define_vertex_type("node")
#         graph.vertices["node"].add_vertices(frame,"from_node",["max_k","cc"])
#         graph.vertices["node"].add_vertices(frame,"to_node",["max_k","cc"])
#         graph.define_edge_type("edge","node","node",directed=True)
#         graph.edges["edge"].add_edges(frame,"from_node","to_node")
#
#         result = graph.graphx_triangle_count(output_property="triangle")
#
#         frame_result = result['node']
#         self.assertEqual(dict(frame_result.schema).has_key('triangle'))
#
#
