import unittest
from mock import MagicMock
from intel_analytics.graph.titan.graph import BulbsGraphWrapper


class TestBulbsGraphWrapper(unittest.TestCase):

    def test_access_graph_attributes(self):
        graph = MagicMock()
        graph.vertices = MagicMock()
        graph.edges = MagicMock()
        graph.client_class = MagicMock()
        wrapper = BulbsGraphWrapper(graph)
        self.assertEqual(wrapper.vertices, graph.vertices)
        self.assertEqual(wrapper.edges, graph.edges)
        self.assertEqual(wrapper.client_class, graph.client_class)

    def test_raise_exception_in_vertices_remove_properties(self):
        graph = MagicMock()
        graph.vertices = MagicMock()
        graph.edges = MagicMock()
        wrapper = BulbsGraphWrapper(graph)
        self.assertRaises(Exception, wrapper.vertices.remove_properties, 1)

    def test_raise_exception_in_edges_remove_properties(self):
        graph = MagicMock()
        graph.vertices = MagicMock()
        graph.edges = MagicMock()
        wrapper = BulbsGraphWrapper(graph)
        self.assertRaises(Exception, wrapper.edges.remove_properties, 1)


if __name__ == '__main__':
    unittest.main()
