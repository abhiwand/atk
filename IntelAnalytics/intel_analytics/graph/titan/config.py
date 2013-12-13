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
Titan/Rexster-specific configuration

Includes the Rexster XML config file template and Graph Builder XML template
"""
from intel_analytics.config import global_config as config
from xml.etree.ElementTree import ElementTree, fromstring

__all__ = ['titan_config']


class TitanConfig(object):
    """
    Config methods specifically for working with Titan, Rexster
    """

    def write_gb_cfg(self, table_name):
        """
        Writes a GraphBuilder config XML file

        Parameters
        ----------
        table_name : string
            name of destination table in Titan

        Returns
        -------
        filename : string
            full path of the config file created
        """
        if not table_name or not table_name.endswith('_titan'):
            raise Exception("Internal error: bad graph table")
        filename = config['graph_builder_titan_xml']
        with open(filename, 'w') as out:
            params = {k: config[k] for k in gb_keys}
            params['titan_storage_tablename'] = table_name
            out.write("<!-- Auto-generated Graph Builder cfg file -->\n\n")
            out.write("<configuration>\n")
            keys = sorted(params.keys())
            for k in keys:
                out.write("  <property>\n    <name>graphbuilder.")
                out.write(k)
                out.write("</name>\n    <value>")
                out.write(params[k])
                out.write("</value>\n  </property>\n")
            out.write("</configuration>\n")
        return filename

    def get_rexster_server_uri(self, table_name):
        return '{0}:{1}/graphs/{2}'.format(
            config['rexster_baseuri'],
            config['rexster_bulbs_port'],
            table_name)

    def rexster_xml_delete_graph(self, titan_table_name):
        tree = ElementTree()
        tree.parse(config['rexster_xml'])
        graphs = tree.find("./graphs")
        xpath = "./graph[graph-name='" + titan_table_name + "']"
        graph = graphs.find(xpath)
        if len(graph):
            graphs.remove(graph)
            tree.write(config['rexster_xml'])
            return True
        return False

    def rexster_xml_add_graph(self, titan_table_name):
        return self.rexster_xml_add_graphs([titan_table_name])

    def rexster_xml_add_graphs(self, titan_table_names):
        """
        Add graph to the Rexster config XML file

        Parameters
        ----------
        table_name : string
           name of destination table in Titan
        """
        tree = ElementTree()
        tree.parse(config['rexster_xml'])

        graphs = tree.find("./graphs")
        params = {k: config[k] for k in rexster_keys}
        for name in titan_table_names:
            # check for duplicate entry
            xpath = "./graph[graph-name='" + name + "']"
            graph = graphs.find(xpath)
            if graph is None:
                params['titan_storage_tablename'] = name
                s = Template(rexster_xml_graph_template_str).substitute(params)
                graph = fromstring(s)
                graphs.append(graph)
        if 'titan_storage_tablename' in params:
            del params['titan_storage_tablename']
        tree.write(config['rexster_xml'])

    # Here the start/stop rexster server code, should we need it:
    # from intel_analytics.table.hbase.table import hbase_registry
    # from intel_analytics.subproc import call
    #
    # def refresh_rexster_cfg(self):
    #     """Refreshes Rexster's configuration file to the hbase registry"""
    #     self._stop_rexster()
    #     self._generate_rexster_xml()
    #     self._start_rexster()
    #
    # def _start_rexster(self):
    #     call(config['rexster_start_cmd'], shell=True)
    #
    # def _stop_rexster(self):
    #     call(config['rexster_stop_cmd'], shell=True)
    #
    # def _generate_rexster_xml(self):
    #     self._rexster_xml_clear_graphs()
    #     values = hbase_registry.values()
    #     titan_table_names = (for value in values if value.endswith('_titan'))
    #     self.rexster_xml_add_graphs(titan_table_names)
    #
    # def _rexster_xml_clear_graphs(self):
    #     tree = ElementTree()
    #     tree.parse(config['rexster_xml'])
    #     graphs = tree.find("./graphs")
    #     for child in graphs:
    #         graphs.remove(child)
    #     tree.write(config['rexster_xml'])

#--------------------------------------------------------------------------
# Rexster
#--------------------------------------------------------------------------
rexster_xml_graph_template_str = """        <graph>
            <graph-name>${titan_storage_tablename}</graph-name>
            <graph-type>com.thinkaurelius.titan.tinkerpop.rexster.TitanGraphConfiguration</graph-type>
            <graph-location></graph-location>
            <graph-read-only>false</graph-read-only>
            <properties>
                <storage.backend>${titan_storage_backend}</storage.backend>
                <storage.hostname>${titan_storage_hostname}</storage.hostname>
                <storage.port>${titan_storage_port}</storage.port>
                <storage.tablename>${titan_storage_tablename}</storage.tablename>
            </properties>
            <extensions>
                <allows>
                    <allow>tp:gremlin</allow>
                </allows>
            </extensions>
        </graph>
"""

from string import Template
from collections import defaultdict

# pull the required keys from the template
d = defaultdict(lambda: None)
Template(rexster_xml_graph_template_str).substitute(d)
# remove the keys whose values are dynamically supplied:
del d['titan_storage_tablename']
rexster_keys = d.keys()
rexster_keys.sort()

#--------------------------------------------------------------------------
# GraphBuilder
#--------------------------------------------------------------------------
gb_keys = ['conf_folder']
for k in ['hostname', 'backend', 'port', 'connection_timeout']:
    gb_keys.append('titan_storage_' + k)
gb_keys.sort()


titan_config = TitanConfig()
