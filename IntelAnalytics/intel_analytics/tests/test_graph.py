import unittest
from mock import patch, MagicMock
from intel_analytics.graph.biggraph import GraphWrapper



class TestGraph(unittest.TestCase):
    def test_convert_query_statements_to_xml(self):
        statements = []
        statements.append("g.V('_gb_ID','11').out")
        statements.append("g.V('_gb_ID','11').outE")

        xml = GraphWrapper._get_query_xml(statements)
        expected = "<query><statement>g.V('_gb_ID','11').out.transform('{[it,it.map()]}')</statement><statement>g.V('_gb_ID','11').outE.transform('{[it,it.map()]}')</statement></query>"
        self.assertEqual(xml, expected)

    def test_get_schema_xml(self):
        schema = {}
        schema['etl-cf:edge_type'] = 'chararray'
        schema['etl-cf:weight'] = 'long'


        xml = GraphWrapper._get_schema_xml(schema)
        expected = "<schema><feature name=\"etl-cf:weight\" type=\"long\" /><feature name=\"etl-cf:edge_type\" type=\"chararray\" /></schema>"
        self.assertEqual(xml, expected)

    @patch('intel_analytics.graph.biggraph.call')
    def test_export_sub_graph_as_graphml(self, call_method):
        call_method.return_value = None
        result_holder = {}
        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        call_method.side_effect = call_side_effect
        graph = MagicMock()
        graph.vertices = MagicMock()
        graph.edges = MagicMock()
        graph.titan_table_name = "test_graph_table"
        wrapper = GraphWrapper(graph)
        statements = []
        wrapper._get_query_xml = MagicMock(return_value = "<query><statement>g.V('_gb_ID','11').out.map</statement></query>")
        wrapper._get_vertex_schema_xml = MagicMock(return_value = "<schema><feature name=\"_gb_ID\" type=\"long\"/><feature name=\"etl-cf:vertex_type\" type=\"chararray\"/></schema>")
        wrapper._get_edge_schema_xml = MagicMock(return_value = "<schema><feature name=\"etl-cf:edge_type\" type=\"chararray\"/><feature name=\"etl-cf:weight\" type=\"long\"/></schema>")
        wrapper.export_sub_graph_as_graphml(statements, "output.xml")

        self.assertEqual("<query><statement>g.V('_gb_ID','11').out.map</statement></query>", result_holder["call_args"][result_holder["call_args"].index('-q') + 1])
        self.assertEqual("<schema><feature name=\"_gb_ID\" type=\"long\"/><feature name=\"etl-cf:vertex_type\" type=\"chararray\"/></schema>", result_holder["call_args"][result_holder["call_args"].index('-v') + 1])
        self.assertEqual("<schema><feature name=\"etl-cf:edge_type\" type=\"chararray\"/><feature name=\"etl-cf:weight\" type=\"long\"/></schema>", result_holder["call_args"][result_holder["call_args"].index('-e') + 1])


if __name__ == '__main__':
    unittest.main()
