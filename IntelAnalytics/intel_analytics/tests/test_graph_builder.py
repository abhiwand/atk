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
import StringIO

curdir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(
    os.path.join(os.path.join(curdir, os.pardir), os.pardir)))


# mock out bulbs import
sys.modules['bulbs.titan'] = __import__('mock_bulbs_titan')
sys.modules['bulbs.config'] = __import__('mock_bulbs_config')
sys.modules['intel_analytics.config'] = __import__('mock_config')
sys.modules['intel_analytics.table.hbase.table'] =\
    __import__('mock_hbase_table')

# mock HBase Registry
from intel_analytics.table.hbase.table import hbase_registry
r = {'f1': 'f1_time',
     'g1': 'g1_f1_time_titan',
     'f2': 'f2_time',
     'g2': 'g2_f2_time_titan',
     'f3': 'f3_time',
     # no g3 on purpose
     }

for k, v in r.items():
    hbase_registry[k] = v

from intel_analytics.graph.biggraph import GraphTypes
from intel_analytics.config import global_config as config

config['conf_folder'] = os.path.join(curdir, "conf")
tmp_folder = os.path.join(curdir, "tmp")
config['logs_folder'] = tmp_folder
config['rexster_xml'] = os.path.join(config['conf_folder'], 'rexster.xml')
config['graphbuilder_titan_xml'] = \
    os.path.join(config['conf_folder'], "graphbuilder_titan.xml")

#print config['conf_folder']
#print config['rexster_xml']

from intel_analytics.graph.titan.graph import TitanGraphBuilderFactory
from intel_analytics.graph.titan.config import titan_config


class TestGraphBuilder(unittest.TestCase):
    def test_get_graph_builder(self):
        factory = TitanGraphBuilderFactory()
        gb = factory.get_graph_builder(GraphTypes.Bipartite)
        self.assertIsNotNone(gb)
        gb = factory.get_graph_builder(GraphTypes.Property)
        self.assertIsNotNone(gb)
        try:
            factory.get_graph_builder(None)
            self.fail("Expected exception for a None name")
        except Exception as e:
            self.assertTrue(str(e).startswith("Unsupported graph type"))

    def test_get_graph(self):
        factory = TitanGraphBuilderFactory()
        graph_name = "g1"
        table_name = "g1_f1_time_titan"
        graph = factory.get_graph(graph_name)
        self.assertEqual(graph_name, graph.user_graph_name)
        self.assertEqual(table_name, graph.titan_table_name)

    def test_get_graph_bad(self):
        factory = TitanGraphBuilderFactory()
        try:
            factory.get_graph("g7")
            self.fail("Expected not found exception for graph name 'g7'")
        except KeyError as e:
            self.assertEqual(e.message,
                             "Could not find titan table name for graph 'g7'")

    def test_get_graph_names(self):
        factory = TitanGraphBuilderFactory()
        names = factory.get_graph_names()
        self.assertTrue({'g1', 'g2'}, set(names))

    def test_activate_graph(self):
        factory = TitanGraphBuilderFactory()
        factory.activate


class TestGraphConfig(unittest.TestCase):

    def test_write_gb_config(self):
        titan_config.write_gb_cfg('g1_f1_timeA_titan')
        with open(config['graphbuilder_titan_xml']) as f:
            result1 = f.read()
        s = StringIO.StringIO()
        titan_config.write_gb_cfg('g1_f1_timeA_titan', stream=s)
        result2 = s.getvalue()

        expected = """<!-- Auto-generated GraphBuilder cfg file -->

<configuration>
  <property>
    <name>graphbuilder.conf_folder</name>
    <value>conf</value>
  </property>
  <property>
    <name>graphbuilder.titan_storage_backend</name>
    <value>mocked</value>
  </property>
  <property>
    <name>graphbuilder.titan_storage_connection_timeout</name>
    <value>mocked</value>
  </property>
  <property>
    <name>graphbuilder.titan_storage_hostname</name>
    <value>mocked</value>
  </property>
  <property>
    <name>graphbuilder.titan_storage_port</name>
    <value>mocked</value>
  </property>
  <property>
    <name>graphbuilder.titan_storage_tablename</name>
    <value>g1_f1_timeA_titan</value>
  </property>
</configuration>
"""
        self.assertEqual(expected, result1)
        self.assertEqual(expected, result2)

    def test_write_gb_config_none(self):
        try:
            titan_config.write_gb_cfg(None)
            self.fail("Expected exception for a None name")
        except Exception as e:
            self.assertEqual(str(e), "table_name is None")

    def test_write_rexster_cfg(self):
        titan_config.write_rexster_cfg()
        with open(config['rexster_xml']) as f:
            result1 = f.read()
        s = StringIO.StringIO()
        titan_config.write_rexster_cfg(stream=s)
        result2 = s.getvalue()
        with open(os.path.join(curdir, 'gold_rexster.xml')) as f:
            expected = f.read()
        self.assertEqual(expected, result1)
        self.assertEqual(expected, result2)


if __name__ == '__main__':
    unittest.main()
