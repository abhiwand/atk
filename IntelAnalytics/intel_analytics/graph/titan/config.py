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
        Writes a GraphBuilder config XML file.

        Parameters
        ----------
        table_name : string
            Then name of the destination table in Titan

        Returns
        -------
        filename : String
            The full path of the config file created by this method.
        """
        if not table_name or not table_name.endswith('_titan'):
            raise Exception("Internal error: bad graph table")
        # TODO: only one Titan config at a time is currently supported?
        filename = config['graph_builder_titan_xml']
        with open(filename, 'w') as out:
            out.write("<!-- Auto-generated Graph Builder cfg file -->\n")
            out.write(self._get_graphbuilder_template().substitute({
                'graphbuilder_titan_ids_block_size': config['titan_ids_block_size'],
                'graphbuilder_titan_ids_partition': config['titan_ids_partition'],
                'graphbuilder_titan_ids_num_partitions': config['titan_ids_num_partitions'],
                'graphbuilder_titan_ids_renew_timeout': config['titan_ids_renew_timeout'],
                'graphbuilder_titan_storage_attempt_wait': config['titan_storage_attempt_wait'],
                'graphbuilder_titan_storage_backend': config['titan_storage_backend'],
                'graphbuilder_titan_storage_batch_loading': config['titan_storage_batch_loading'],
                'graphbuilder_titan_storage_connection_timeout': config['titan_storage_connection_timeout'],
                'graphbuilder_titan_storage_hostname': config['titan_storage_hostname'],
                'graphbuilder_titan_storage_idauthority_retries': config['titan_storage_idauthority_retries'],
                'graphbuilder_titan_storage_idauthority_wait_time': config['titan_storage_idauthority_wait_time'],
                'graphbuilder_titan_storage_port': config['titan_storage_port'],
                'graphbuilder_titan_storage_tablename': table_name,
                'graphbuilder_titan_storage_write_attempts': config['titan_storage_write_attempts'],
                'mapred_job_reuse_jvm_num_tasks': config['graphbuilder_mapred_job_reuse_jvm_num_tasks'],
                'mapred_map_tasks_speculative_execution': config['graphbuilder_mapred_map_tasks_speculative_execution'],
                'mapred_max_split_size': config['graphbuilder_mapred_max_split_size'],
                'mapred_reduce_tasks': config['graphbuilder_mapred_reduce_tasks'],
                'mapred_reduce_tasks_speculative_execution': config['graphbuilder_mapred_reduce_tasks_speculative_execution']
            }))
        return filename

    def _get_graphbuilder_template(self):
        """
        Get the graphbuilder-config-template.xml as a Template
        """
        filename = config['graph_builder_config_template']
        template_file = open(filename, 'r')
        return Template(template_file.read())

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
        if graph is not None and len(graph):
            graphs.remove(graph)
            tree.write(config['rexster_xml'])
            return True
        return False

    def rexster_xml_add_graph(self, titan_table_name):
        return self.rexster_xml_add_graphs([titan_table_name])

    def rexster_xml_add_graphs(self, titan_table_names):
        """
        Adds a graph to the Rexster config XML file

        Parameters
        ----------
        table_names : strings
           The names of the destination tables in Titan.
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

titan_config = TitanConfig()
