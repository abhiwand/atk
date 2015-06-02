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
# class KcliqueTest(unittest.TestCase):
#     def kclique(self):
#         print "define csv file"
#         noun_graph_data ="datasets/noun_graph_small.csv"
#         schema = [("source",str),("target",str)]
#         noun_words_frame = ia.Frame(ia.CsvFile(noun_graph_data,schema))
#         graph = ia.Graph()
#
#         graph.define_vertex_type("source")
#         graph.vertices["source"].add_vertices(noun_words_frame,"source")
#         graph.vertices["source"].add_vertices(noun_words_frame,"target")
#
#         graph.define_edge_type("edge", "source", "source", False)
#         graph.edges["edge"].add_edges(noun_words_frame,"source","target")
#
#         output = graph.ml.kclique_percolation(clique_size = 3, community_property_label = "community")
#         output_dictionary = output['vertex_dictionary']
#
#         self.assertTrue('source' in output_dictionary)
#
# if __name__ == "__main__":
#     unittest.main()
