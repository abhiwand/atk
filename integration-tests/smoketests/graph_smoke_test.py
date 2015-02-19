##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

import unittest
import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class GraphSmokeTest(unittest.TestCase):
    """
    Smoke test basic graph operations to verify functionality that will be needed by all other tests.

    If these tests don't pass, there is no point in running other tests.

    This is a build-time test so it needs to be written to be as fast as possible:
        - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
        - Tests are ran in parallel
        - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_graph(self):
        print "define csv file"
        csv = ia.CsvFile("/datasets/movie.csv", schema= [('user', ia.int32),
                                            ('vertex_type', str),
                                            ('movie', ia.int32),
                                            ('rating', ia.int32),
                                            ('splits', str)])

        print "creating frame"
        frame = ia.Frame(csv)

        # TODO: add asserts verifying inspect is working
        print
        print frame.inspect(20)
        print
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        #self.assertEqual(frame.column_names, ['', '', '', '', ''])
        self.assertEquals(len(frame.column_names), 5, "frame should have 5 columns")

        print "create graph"
        graph = ia.Graph()

        self.assertIsNotNone(graph._id)
        self.assertTrue(graph.name.startswith('graph'), "name didn't start with 'graph' " + graph.name)

        print "define vertices and edges"
        graph.define_vertex_type('movies')
        graph.define_vertex_type('users')
        graph.define_edge_type('ratings', 'users', 'movies', directed=True)
        self.assertEquals(graph.vertices['users'].row_count, 0, "making sure newly defined vertex frame does not have rows")
        self.assertEquals(graph.vertices['movies'].row_count, 0, "making sure newly defined vertex frame does not have rows")
        self.assertEquals(graph.edges['ratings'].row_count, 0, "making sure newly defined edge frame does not have rows")
        #self.assertEquals(graph.vertex_count, 0, "no vertices expected yet")
        #self.assertEquals(graph.edge_count, 0, "no edges expected yet")

        print "add_vertices() users"
        graph.vertices['users'].add_vertices( frame, 'user', [])

        # TODO: add asserts verifying inspect is working
        print
        print graph.vertices['users'].inspect(20)
        print
        self.assertEquals(graph.vertices['users'].row_count, 13)
        self.assertEquals(len(graph.vertices['users'].column_names), 3)
        #self.assertEquals(graph.vertices['users'].row_count, graph.vertex_count, "row count of user vertices should be same as vertex count on graph")

        print "add_vertices() movies"
        graph.vertices['movies'].add_vertices( frame, 'movie', [])
        self.assertEquals(graph.vertices['users'].row_count, 13)
        self.assertEquals(graph.vertices['movies'].row_count, 11)
        self.assertEquals(len(graph.vertices['users'].column_names), 3)
        self.assertEquals(len(graph.vertices['movies'].column_names), 3)
        #self.assertEquals(graph.vertex_count, 24, "vertex_count should be the total number of users and movies")

        print "add_edges()"
        graph.edges['ratings'].add_edges(frame, 'user', 'movie', ['rating'], create_missing_vertices=False)
        self.assertEquals(len(graph.edges['ratings'].column_names), 5)
        self.assertEquals(graph.edges['ratings'].row_count, 20, "expected 20 rating edges")

if __name__ == "__main__":
    unittest.main()
