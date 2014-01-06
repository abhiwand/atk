##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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
from intel_analytics.graph.titan import render

import unittest
import json
from mock import Mock


class TestConversionFunctions(unittest.TestCase):

#    def setUp(self):
#        self.seq = range(10)

    def setUp(self):
        start = Mock()
        start.data.return_value = {'name': u'start', 'other_label': u'this is it'}
        start.eid = 10
        vertex = Mock()
        vertex.eid = 35
        vertex.data.return_value = {'name': u'end'}
        vertex.outE.return_value = []
        edge = Mock()
        edge.eid = 12
        edge.label.return_value = u'knows'
        edge.inV.return_value = vertex
        edge.outV.return_value = start
        start.outE.return_value = [edge]
        start.outV.return_value = [vertex]
        self.__dict__.update(locals())

    def test_edge_to_json(self):
        res = render.edge_to_json(self.edge)
        exp = {'nodeTo': 35, 'data': {'label': u'knows'}}
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_traversal_depth_0(self):
        trav = render.traverse(self.start, depth=0)
        self.assertEqual([(self.start, [])], list(trav))

    def test_traversal_depth_1(self):
        trav = render.traverse(self.start, depth=1)
        self.assertEqual([(self.start, [self.edge]), (self.vertex, [])], list(trav))

    def test_traversal_depth_1_w_vfilter(self):
        trav = render.traverse(self.start, depth=1, vertex_filter=lambda x: False)
        self.assertEqual([], list(trav))

    def test_traversal_depth_1_w_efilter(self):
        trav = render.traverse(self.start, depth=1, edge_filter=lambda x: False)
        self.assertEqual([(self.start, [])], list(trav))


    def test_vertex_to_json_no_edges(self):
        trav = render.traverse(self.start, depth=0)
        res = render._traversal_to_json(trav)
        exp = [
            {'id': 10, 'name': u'start', 'data': {'name': u'start', 'other_label': u'this is it'}, 'adjacencies': []}]
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_vertex_to_json_with_edges(self):
        trav = render.traverse(self.start, depth=1)
        res = render._traversal_to_json(trav)
        exp = [{'id': 10, 'name': u'start', 'data': {'name': u'start', 'other_label': u'this is it'},
                'adjacencies': [{'nodeTo': 35, 'data': {'label': u'knows'}}]},
               {'id': 35, 'name': u'end', 'data': {'name': u'end'}, 'adjacencies': []}]
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_vertex_to_json_with_label(self):
        trav = render.traverse(self.start, depth=1)
        res = render._traversal_to_json(trav, vertex_label='other_label')
        exp = [{'id': 10, 'name': u'this is it', 'data': {'name': u'start', 'other_label': 'this is it'},
                'adjacencies': [{'nodeTo': 35, 'data': {'label': u'knows'}}]},
               {'id': 35, 'name': u'35', 'data': {'name': u'end'}, 'adjacencies': []}]
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_vertex_to_json_with_vformat(self):
        trav = render.traverse(self.start, depth=1)
        res = render._traversal_to_json(trav, vertex_label='other_label', vertex_formatter=formatter)
        exp = [{'id': 10, 'name': u'this is it', 'data': {'name': u'start', 'other_label': 'this is it', 'z': '42'},
                'adjacencies': [{'nodeTo': 35, 'data': {'label': u'knows'}}]},
               {'id': 35, 'name': u'35', 'data': {'name': u'end', 'z': '42'}, 'adjacencies': []}]
        self.assertEqual(json.dumps(res), json.dumps(exp))


def formatter(vertex, edges, vertex_label, edge_formatter):
    js = render.vertex_to_json(vertex, edges, vertex_label, edge_formatter)
    js['data']['z'] = '42'
    return js


if __name__ == '__main__':
    unittest.main()
