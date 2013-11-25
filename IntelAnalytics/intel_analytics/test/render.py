from intel_analytics.graph.titan import render

import unittest
import json
from mock import Mock

class TestConversionFunctions(unittest.TestCase):

#    def setUp(self):
#        self.seq = range(10)

    def setUp(self):
	start = Mock()
	start.data.return_value = {'name':u'start', 'other_label':u'this is it'}
	start.eid = 10
        vertex = Mock()
	vertex.eid = 35
	vertex.data.return_value = {'name':u'end'}
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
	exp = {'nodeTo':35, 'data': {'label':u'knows'}}        
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_traversal_depth_0(self):
	trav = render.traverse(self.start, depth = 0)
	self.assertEqual([(self.start,[])], list(trav))

    def test_traversal_depth_1(self):
 	trav = render.traverse(self.start, depth = 1)
	self.assertEqual([(self.start,[self.edge]),(self.vertex,[])], list(trav))

    def test_traversal_depth_1_w_vfilter(self):
 	trav = render.traverse(self.start, depth = 1, vertex_filter = lambda x:False)
	self.assertEqual([], list(trav))

    def test_traversal_depth_1_w_efilter(self):
 	trav = render.traverse(self.start, depth = 1, edge_filter = lambda x:False)
	self.assertEqual([(self.start,[])], list(trav))



    def test_vertex_to_json_no_edges(self):
        trav = render.traverse(self.start, depth = 0)
        res = render._traversal_to_json(trav)
        exp = [{'id':10, 'name':u'start', 'data': {'name':u'start','other_label':u'this is it'}, 'adjacencies':[]}]
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_vertex_to_json_with_edges(self):
        trav = render.traverse(self.start, depth = 1)
        res = render._traversal_to_json(trav)
        exp = [{'id':10, 'name':u'start', 'data': {'name':u'start','other_label':u'this is it'}, 'adjacencies':[{'nodeTo':35, 'data': {'label':u'knows'}}] }, {'id':35, 'name':u'end', 'data': {'name':u'end'}, 'adjacencies':[]}]
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_vertex_to_json_with_label(self):
	trav = render.traverse(self.start, depth = 1)
        res = render._traversal_to_json(trav, vertex_label = 'other_label')
        exp = [{'id':10, 'name':u'this is it', 'data': {'name':u'start', 'other_label':'this is it'}, 'adjacencies':[{'nodeTo':35, 'data': {'label':u'knows'}}] }, {'id':35, 'name':u'35', 'data': {'name':u'end'}, 'adjacencies':[]}]
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_vertex_to_json_with_vformat(self):
	trav = render.traverse(self.start, depth = 1)
        res = render._traversal_to_json(trav, vertex_label = 'other_label', vertex_formatter = test_format)
        exp = [{'id':10, 'name':u'this is it', 'data': {'name':u'start', 'other_label':'this is it', 'z':'42'}, 'adjacencies':[{'nodeTo':35, 'data': {'label':u'knows'}}] }, {'id':35, 'name':u'35', 'data': {'name':u'end', 'z':'42'}, 'adjacencies':[]}]
        self.assertEqual(json.dumps(res), json.dumps(exp))

def test_format(*args, **kwargs):
    js = render.vertex_to_json(*args, **kwargs)
    js['data']['z'] = '42' 
    return js


if __name__ == '__main__':
    unittest.main()
