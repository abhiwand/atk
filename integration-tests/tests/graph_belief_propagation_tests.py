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
# TODO: Commented out because of ClassNotFound error with spark local mode
# class GraphBeliefPropagationTest(unittest.TestCase):
#     def test_belief_propagation(self):
#         lbp_graphlab_input_data ="/datasets/lbp_graphlab_small.csv"
#         extra_vertex="/datasets/lbp_graphlab_append.csv"
#
#         schema = [("id1",ia.int32),("id2",ia.int32),("prior",str)]
#         schema2 = [("id1",ia.int32),("prior",str)]
#
#         lbp_frame = ia.Frame(ia.CsvFile(lbp_graphlab_input_data,schema))
#         extra_vertex_frame = ia.Frame(ia.CsvFile(extra_vertex,schema2))
#
#         graph = ia.Graph()
#         graph.define_vertex_type("nodes")
#         graph.vertices["nodes"].add_vertices(lbp_frame, "id1",["prior"])
#         graph.vertices["nodes"].add_vertices(extra_vertex_frame, "id1",["prior"])
#
#         graph.define_edge_type("edge","nodes","nodes",directed= False)
#         graph.edges["edge"].add_edges(lbp_frame,"id1","id2")
#
#         result = graph.ml.belief_propagation(prior_property="prior",posterior_property="lbp_output",max_iterations=1)
#
#         frame_result = result['vertex_dictionary']
#         self.assertTrue(dict(frame_result['nodes'].schema).has_key('lbp_output'))
#
# if __name__ == "__main__":
#     unittest.main()
