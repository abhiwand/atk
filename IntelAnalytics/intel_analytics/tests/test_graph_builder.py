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
"""
Unit tests for intel_analytics/graph  graph builder code
"""
import unittest
import os
import sys
from shutil import copyfile
from mock import patch, Mock, MagicMock
from testutils import RegistryCallableFactory, get_diff_str

get_registry_callable = RegistryCallableFactory().get_registry_callable

_here_folder = os.path.dirname(__file__)
sys.path.append(os.path.abspath(
    os.path.join(os.path.join(_here_folder, os.pardir), os.pardir)))


if __name__ == '__main__':
    sys.modules['bulbs.titan'] = __import__('mock_bulbs_titan')
    sys.modules['bulbs.config'] = __import__('mock_bulbs_config')
    sys.modules['intel_analytics.config'] = __import__('mock_config')
    sys.modules['intel_analytics.subproc'] = __import__('mock_subproc')
else:
    #to get coverage on all of our modules we need to execute the unit tests utilizing a test runner
    #this runner executes all of the test files in the same execution space making it so that import from previous
    #files are still in sys.modules we need to do the following to reset the required modules so that imports work as
    #expected.

    modules_to_remove = ['intel_analytics.graph.biggraph', 'intel_analytics.graph.titan.graph', 'intel_analytics.graph.titan.config',
                         'bulbs.titan','bulbs.config','intel_analytics.config', 'intel_analytics.subproc']

    for module in modules_to_remove:
        if module in sys.modules:
            del sys.modules[module]

    #The __import__ function was not resolving due to conflicts between intel_analytics and its submodules. The following import provides the same functionality without the conflicts
    #The .__name__ field is used instead of a hard coded string is used so that intelliJ auto formatting will not remove this import.

    import intel_analytics.tests.mock_bulbs_config, intel_analytics.tests.mock_bulbs_titan, intel_analytics.tests.mock_config, intel_analytics.tests.mock_subproc

    sys.modules['bulbs.titan'] = sys.modules[intel_analytics.tests.mock_bulbs_titan.__name__]
    sys.modules['bulbs.config'] = sys.modules[intel_analytics.tests.mock_bulbs_config.__name__]
    sys.modules['intel_analytics.config'] = sys.modules[intel_analytics.tests.mock_config.__name__]
    sys.modules['intel_analytics.subproc'] = sys.modules[intel_analytics.tests.mock_subproc.__name__]


# mock config
from intel_analytics.config import global_config as config

config['conf_folder'] = os.path.join(_here_folder, "conf")
_tmp_folder = os.path.join(_here_folder, "tmp")
config['logs_folder'] = _tmp_folder
config['rexster_xml'] = os.path.join(_tmp_folder, 'rexster.xml')
config['graph_builder_titan_xml'] = \
    os.path.join(config['conf_folder'], "graph_builder_titan.xml")
config['hbase_names_file'] = \
    os.path.join(config['conf_folder'], "table_name.txt")
config['graph_builder_config_template'] = os.path.join(_here_folder, "graphbuilder-config-template.xml")

from intel_analytics.graph.biggraph import GraphTypes
from intel_analytics.graph.titan.graph import TitanGraphBuilderFactory, build, get_gb_build_command
from intel_analytics.graph.titan.config import titan_config

# mock HBase Registry
registry = {'f1': 'f1_time',
            'g1': 'g1_f1_time_titan',
            'f2': 'f2_time',
            'g2': 'g2_f2_time_titan',
            'f3': 'f3_time',
            # no g3 on purpose
}


class TestGraphBuilder(unittest.TestCase):
    def __init__(self, *args, **kw):
        self._hbase_registries = {}
        super(TestGraphBuilder, self).__init__(*args, **kw)

    @classmethod
    def tearDownClass(cls):
        '''This method will revert the mocked modules so that the test runner can continue executing tests that do not mock modules'''
        if __name__ != '__main__':
            for module in modules_to_remove:
                del sys.modules[module]
        else:
            pass


    def test_get_graph_builder(self):
        factory = TitanGraphBuilderFactory()
        gb = factory.get_graph_builder(GraphTypes.Bipartite, Mock())
        self.assertIsNotNone(gb)
        gb = factory.get_graph_builder(GraphTypes.Property, Mock())
        self.assertIsNotNone(gb)
        try:
            factory.get_graph_builder(None, Mock())
        except Exception as e:
            self.assertTrue(str(e).startswith("Unsupported graph type"))
        else:
            self.fail("Expected exception for a None name")
        try:
            factory.get_graph_builder(GraphTypes.Property, None)
        except Exception as e:
            self.assertTrue(str(e) == "Graph builder has no source")
        else:
            self.fail("Expected exception for a None graph source")

    @patch('intel_analytics.table.hbase.table.hbase_registry',
           new_callable=get_registry_callable('test_get_graph'))
    @patch('intel_analytics.graph.titan.graph.hbase_registry',
           new_callable=get_registry_callable('test_get_graph'))
    def test_get_graph(self, mock_registry, mr2):
        self.assertIs(mock_registry, mr2)
        mock_registry.initialize(registry)
        factory = TitanGraphBuilderFactory()
        graph_name = "g1"
        table_name = "g1_f1_time_titan"
        graph = factory.get_graph(graph_name)
        self.assertEqual(graph_name, graph.user_graph_name)
        self.assertEqual(table_name, graph.titan_table_name)

    @patch('intel_analytics.table.hbase.table.hbase_registry',
           new_callable=get_registry_callable('test_get_graph_bad'))
    @patch('intel_analytics.graph.titan.graph.hbase_registry',
           new_callable=get_registry_callable('test_get_graph_bad'))
    def test_get_graph_bad(self, mock_registry, mr2):
        self.assertIs(mock_registry, mr2)
        mock_registry.initialize(registry)
        factory = TitanGraphBuilderFactory()
        try:
            factory.get_graph("g7")
        except KeyError as e:
            self.assertEqual(e.message,
                             "Could not find titan table name for graph 'g7'")
        else:
            self.fail("Expected not found exception for graph name 'g7'")

    @patch('intel_analytics.table.hbase.table.hbase_registry',
           new_callable=get_registry_callable('test_get_graph_names'))
    @patch('intel_analytics.graph.titan.graph.hbase_registry',
           new_callable=get_registry_callable('test_get_graph_names'))
    def test_get_graph_names(self, mock_registry, mr2):
        self.assertIs(mock_registry, mr2)
        mock_registry.itialize(registry)
        factory = TitanGraphBuilderFactory()
        names = factory.get_graph_names()
        self.assertTrue({'g1', 'g2'}, set(names))

    @patch('intel_analytics.table.hbase.table.hbase_registry',
           new_callable=get_registry_callable('test_build'))
    @patch('intel_analytics.graph.titan.graph.hbase_registry',
           new_callable=get_registry_callable('test_build'))
    def test_build(self, mock_registry, mr2):
        self.assertIs(mock_registry, mr2)
        mock_registry.initialize(registry)
        copyfile(os.path.join(_here_folder, 'gold_rexster.xml'),
                 config['rexster_xml'])
        graph_name = 'g3'
        self.assertFalse(graph_name in mock_registry)
        frame = Mock()
        frame._table.table_name = 'f3_time'
        g = build(graph_name, frame, [], [], False, overwrite=False, append=False, flatten=False)
        self.assertIsNotNone(g)
        self.assertIsNotNone(mock_registry.get_value(graph_name))

    def _raise_intended_exception(self):
        raise Exception('mock intended exception')

    @patch('intel_analytics.table.hbase.table.hbase_registry',
           new_callable=get_registry_callable('test_build_error'))
    @patch('intel_analytics.graph.titan.graph.hbase_registry',
           new_callable=get_registry_callable('test_build_error'))
    @patch('intel_analytics.graph.titan.graph.call')
    def test_build_error(self, mock_call, mock_registry, mr2):
        self.assertIs(mock_registry, mr2)
        mock_registry.initialize(registry)
        copyfile(os.path.join(_here_folder, 'gold_rexster.xml'),
                 config['rexster_xml'])
        graph_name = 'g3'
        mock_call.side_effect = self._raise_intended_exception
        self.assertFalse(graph_name in mock_registry)
        frame = Mock()
        frame._table.table_name = 'f3_time'
        try:
            build(graph_name, frame, [], [], False, overwrite=False, append=False)
        except Exception:
            # make sure graph_name is not in the registry
            self.assertFalse(graph_name in mock_registry)
        else:
            self.fail("Expected error from build call")

    def test_generate_titan_table_name(self):
        from intel_analytics.graph.titan.graph import generate_titan_table_name

        source = Mock()
        source._table.table_name = "table_name"
        result = generate_titan_table_name("prefix", source)
        self.assertEqual("prefix_table_name_titan", result)
        try:
            generate_titan_table_name("prefix", "junk")
        except Exception as e:
            self.assertEqual("Could not get table name from source", str(e))
        else:
            self.fail("Expected error while get table name")

    @patch('intel_analytics.table.hbase.table.hbase_registry',
           new_callable=get_registry_callable('test_builder_bipartite'))
    @patch('intel_analytics.graph.titan.graph.hbase_registry',
           new_callable=get_registry_callable('test_builder_bipartite'))
    def test_graph_builder_bipartite(self, mock_registry, mr2):
        self.assertIs(mock_registry, mr2)
        mock_registry.initialize(registry)
        copyfile(os.path.join(_here_folder, 'gold_rexster.xml'),
                 config['rexster_xml'])
        factory = TitanGraphBuilderFactory()
        frame = MagicMock()
        frame.__str__.return_value = \
            frame._table.table_name = 'bipartite_graph_frame'
        gb = factory.get_graph_builder(GraphTypes.Bipartite, frame)
        gb.register_vertex('colA', [])
        gb.register_vertex('colB', ['colP', 'colQ'])
        expected = """Source: bipartite_graph_frame
Vertices:
colA
colB=colP,colQ"""
        self.assertEqual(expected, gb.__repr__())
        graph_name = "my_bipartite_graph"
        g = gb.build(graph_name)
        self.assertIsNotNone(g)
        self.assertIsNotNone(mock_registry.get_value(graph_name))


    @patch('intel_analytics.table.hbase.table.hbase_registry',
           new_callable=get_registry_callable('test_builder_bipartite'))
    @patch('intel_analytics.graph.titan.graph.hbase_registry',
           new_callable=get_registry_callable('test_builder_bipartite'))
    def test_graph_builder_property(self, mock_registry, mr2):
        self.assertIs(mock_registry, mr2)
        mock_registry.initialize(registry)
        copyfile(os.path.join(_here_folder, 'gold_rexster.xml'),
                 config['rexster_xml'])
        factory = TitanGraphBuilderFactory()
        frame = MagicMock()
        frame.__str__.return_value = \
            frame._table.table_name = 'property_graph_frame'
        gb = factory.get_graph_builder(GraphTypes.Property, frame)
        gb.register_vertex('colA', [])
        gb.register_vertex('colB', ['colP', 'colQ'])
        gb.register_vertices([('colC', ['colR', 'colS']), ('colD', [])])
        gb.register_edge(('colA', 'colB', 'edgeAB'), ['colT'])
        gb.register_edges([(('colC', 'colB', 'edgeCB'), []),
                           (('colD', 'colB', 'edgeDB'), ['colU'])])
        expected = """Source: property_graph_frame
Vertices:
colA
colB=colP,colQ
colC=colR,colS
colD
Edges:
colA,colB,edgeAB,colT
colC,colB,edgeCB
colD,colB,edgeDB,colU"""
        self.assertEqual(expected, gb.__repr__())
        graph_name = "my_property_graph"
        g = gb.build(graph_name)
        self.assertIsNotNone(g)
        self.assertIsNotNone(mock_registry.get_value(graph_name))


    @patch('intel_analytics.graph.titan.graph.get_pig_args_with_gb')
    def test_get_gb_build_command_overwrite(self, get_pig_args_with_gb):
        get_pig_args_with_gb.return_value = [ 'mock_pig' ]
        result = " ".join(get_gb_build_command("gb_config", "table_name", [], [], is_directed=False, overwrite=True, append=False, flatten=False))

        self.assertEqual("mock_pig -t table_name -c gb_config -e  -v  -o is_overwrite", result)
        get_pig_args_with_gb.assert_called_once_with('pig_load_titan.py')

    @patch('intel_analytics.graph.titan.graph.get_pig_args_with_gb')
    def test_get_gb_build_command_append(self, get_pig_args_with_gb):
        get_pig_args_with_gb.return_value = [ 'mock_pig' ]
        result = " ".join(get_gb_build_command("gb_config", "table_name", [], [], is_directed=False, overwrite=False, append=True, flatten=False))

        self.assertEqual("mock_pig -t table_name -c gb_config -e  -v  -a is_append", result)
        get_pig_args_with_gb.assert_called_once_with('pig_load_titan.py')

    @patch('intel_analytics.graph.titan.graph.get_pig_args_with_gb')
    def test_get_gb_build_command_overwrite_append(self, get_pig_args_with_gb):
        get_pig_args_with_gb.return_value = [ 'goat_disguised_as_pig' ]
        result = " ".join(get_gb_build_command("gb_config", "table_name", [], [], is_directed=False, overwrite=True, append=True, flatten=False))

        self.assertEqual("goat_disguised_as_pig -t table_name -c gb_config -e  -v  -a is_append -o is_overwrite", result)
        get_pig_args_with_gb.assert_called_once_with('pig_load_titan.py')

    @patch('intel_analytics.graph.titan.graph.get_pig_args_with_gb')
    def test_get_gb_build_command_is_directed(self, get_pig_args_with_gb):
        get_pig_args_with_gb.return_value = [ 'lamb' ]
        result = " ".join(get_gb_build_command("config1", "table2", [], [], is_directed=True, overwrite=False, append=False, flatten=False))

        self.assertEqual("lamb -t table2 -c config1 -e  -v  -d is_directed", result)
        get_pig_args_with_gb.assert_called_once_with('pig_load_titan.py')

    @patch('intel_analytics.graph.titan.graph.get_pig_args_with_gb')
    def test_get_gb_build_command_flatten(self, get_pig_args_with_gb):
        get_pig_args_with_gb.return_value = [ 'mock_pig' ]
        result = " ".join(get_gb_build_command("config1", "table2", [], [], is_directed=False, overwrite=False, append=True, flatten=True))

        self.assertEqual("mock_pig -t table2 -c config1 -e  -v  -a is_append -f is_flatten", result)
        get_pig_args_with_gb.assert_called_once_with('pig_load_titan.py')


class TestGraphConfig(unittest.TestCase):
    def test_write_gb_config(self):
        titan_config.write_gb_cfg('g1_f1_timeA_titan')
        with open(config['graph_builder_titan_xml']) as f:
            result = f.read()
        expected = """<!-- Auto-generated Graph Builder cfg file -->
<!--
    The configuration parameters are documented at https://github.com/thinkaurelius/titan/wiki/Using-HBase
    Currently GB 2.0 (alpha) supports only HBase as the storage backend.
  -->
<configuration>
    <property>
        <name>graphbuilder.titan_ids.block-size</name>
        <value>mocked</value>
        <description>
        </description>
    </property>
</configuration>
"""
        if expected != result:
            msg = get_diff_str(expected, result)
            self.fail(msg)

    def test_write_gb_config_none(self):
        try:
            titan_config.write_gb_cfg(None)
        except Exception as e:
            self.assertEqual(str(e), "Internal error: bad graph table")
        else:
            self.fail("Expected exception for a None name")

    def test_rexster_xml_add_graph(self):
        rexster_xml = config['rexster_xml']
        copyfile(os.path.join(_here_folder, 'gold_rexster.xml'), rexster_xml)
        titan_config.rexster_xml_add_graph('newgraph_f2_time_titan')
        expected = ['g1_f1_time_titan',
                    'g2_f2_time_titan',
                    'newgraph_f2_time_titan']
        self._validate_rexster_xml(expected)

    def test_rexster_xml_delete_graph(self):
        rexster_xml = config['rexster_xml']
        from shutil import copyfile

        copyfile(os.path.join(_here_folder, 'gold_rexster.xml'), rexster_xml)
        self.assertTrue(
            titan_config.rexster_xml_delete_graph('g2_f2_time_titan'))
        expected = ['g1_f1_time_titan']
        self._validate_rexster_xml(expected)
        self.assertFalse(
            titan_config.rexster_xml_delete_graph('nonexistent_time_titan'))

    def _validate_rexster_xml(self, expected):
        from xml.etree.ElementTree import ElementTree

        tree = ElementTree()
        tree.parse(config['rexster_xml'])
        graph_elems = tree.findall("graphs/graph")
        self.assertEqual(len(expected), len(graph_elems))
        for i in range(len(expected)):
            name = graph_elems[i].find("graph-name").text
            self.assertEqual(expected[i], name)
            name2 = graph_elems[i].find("./properties/storage.tablename").text
            self.assertEqual(name, name2)


if __name__ == '__main__':
    unittest.main()
