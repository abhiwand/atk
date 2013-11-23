from intel_analytics.graph.titan import render

import unittest
import json
from mock import Mock

class TestConversionFunctions(unittest.TestCase):

#    def setUp(self):
#        self.seq = range(10)

    def setUp(self):
	start = Mock()
	start.data.return_value = {'name':u'start'}
	start.eid = 10
        vertex = Mock()
	vertex.eid = 35
	vertex.data.return_value = {'name':u'end'}
	edge = Mock()
	edge.eid = 12
	edge.label.return_value = u'knows'
        edge.inV.return_value = vertex
	start.outE.return_value = [edge]
	start.outV.return_value = [vertex]
	self.__dict__.update(locals())
 
    def test_edge_to_json(self):
        res = render.edge_to_json(self.edge)
	exp = {'nodeTo':35, 'data': {'label':u'knows'}}        
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_vertex_to_json_0(self):
        res = render.vertex_to_json(self.start, depth = 0)
        exp = [{'id':10, 'name':u'start', 'data': {'name':u'start'}, 'adjacencies':[]}]
        self.assertEqual(json.dumps(res), json.dumps(exp))

    def test_vertex_to_json_1(self):
        res = render.vertex_to_json(self.start, depth = 1)
        exp = [{'id':10, 'name':u'start', 'data': {'name':u'start'}, 'adjacencies':[{'nodeTo':35, 'data': {'label':u'knows'}}] }, {'id':35, 'name':u'end', 'data': {'name':u'end'}, 'adjacencies':[]}]
        self.assertEqual(json.dumps(res), json.dumps(exp))



if __name__ == '__main__':
    unittest.main()
