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
The Titan-specific graph implementation.
"""

__all__ = []

from intel_analytics.graph.biggraph import \
    PropertyGraphBuilder, BipartiteGraphBuilder,\
    GraphBuilderEdge, GraphBuilderFactory, GraphTypes

from intel_analytics.graph.titan.ml import TitanGiraphMachineLearning
from intel_analytics.graph.titan.config import titan_config
from intel_analytics.table.hbase.table import hbase_registry
from intel_analytics.subproc import call
from intel_analytics.config import global_config

from bulbs.titan import Graph as bulbsGraph
from bulbs.config import Config as bulbsConfig
from intel_analytics.report import ProgressReportStrategy
from intel_analytics.logger import stdout_logger as logger


#class TitanGraph(object):   # TODO: inherit BigGraph later
#    """
#    proxy for a graph stored in Titan
#    """
#    def __init__(self):
#        self.ml = TitanGiraphMachineLearning(self)


#-----------------------------------------------------------------------------
# HBase to Titan graph building
#-----------------------------------------------------------------------------
class TitanGraphBuilderFactory(GraphBuilderFactory):
    """
    Provides a set of Titan graph builders.
    """
    def __init__(self):
        super(TitanGraphBuilderFactory, self).__init__()
        self._active_titan_table_name = None

    def get_graph_builder(self, graph_type, source):
        if source is None:
            raise Exception("Graph builder has no source")
        if graph_type is GraphTypes.Bipartite:
            return HBase2TitanBipartiteGraphBuilder(source)
        elif graph_type is GraphTypes.Property:
            return HBase2TitanPropertyGraphBuilder(source)
        else:
            raise Exception("Unsupported graph type: " + str(graph_type))

    def get_graph(self, graph_name):
        titan_table_name = self._get_titan_table_name(graph_name)
        return self._get_graph(graph_name, titan_table_name)

    def get_graph_names(self):
        return (k for k, v in hbase_registry.items() if v.endswith('_titan'))

    def _get_titan_table_name(self, graph_name):
        try:
            titan_table_name = hbase_registry[graph_name]
        except KeyError:
            raise KeyError("Could not find titan table name for graph '"
                           + graph_name + "'")
        if not titan_table_name.endswith("_titan"):
            raise Exception("Internal error: graph name "
                            + graph_name + " not mapped to graph")
        return titan_table_name

    def _get_graph(self, graph_name, titan_table_name):
        rexster_uri = titan_config.get_rexster_server_uri(titan_table_name)
        bulbs_config = bulbsConfig(rexster_uri)
        titan_graph = bulbsGraph(bulbs_config)
        titan_graph.user_graph_name = graph_name
        titan_graph.titan_table_name = titan_table_name
        titan_graph.ml = TitanGiraphMachineLearning(titan_graph)
        return titan_graph

    @staticmethod
    def get_instance():
        global titan_graph_builder_factory
        return titan_graph_builder_factory

# global singleton instance
titan_graph_builder_factory = TitanGraphBuilderFactory()


class HBase2TitanBipartiteGraphBuilder(BipartiteGraphBuilder):
    """
    The bipartite graph builder for HBase->Titan.
    """
    def __init__(self, source):
        super(HBase2TitanBipartiteGraphBuilder, self).__init__(source)

    def __repr__(self):
        s = "Source: " \
            + (str(self._source) if self._source is not None else "None")
        if len(self._vertex_list) > 0:
            s += '\nVertices:\n' + \
                '\n'.join(map(lambda x: vertex_str(x, True), self._vertex_list))
        return s

    def build(self, graph_name, overwrite=False):
        if len(self._vertex_list) != 2:
            raise ValueError("ERROR: bipartite graph construction requires 2 " +
                "vertex sources; " + str(len(self._vertex_list)) + " detected")

        # create the one edge source for bipartite
        edge_list = [GraphBuilderEdge(
            (self._vertex_list[0].key, self._vertex_list[1].key, '_no_label'))]

        return build(graph_name,
                     self._source,
                     self._vertex_list,
                     edge_list,
                     is_directed=False,
                     overwrite=overwrite)


class HBase2TitanPropertyGraphBuilder(PropertyGraphBuilder):
    """
    The property graph builder for HBase->Titan.
    """
    def __init__(self, source):
        super(HBase2TitanPropertyGraphBuilder, self).__init__(source)

    def __repr__(self):
        s = "Source: "\
            + (str(self._source) if self._source is not None else "None")
        if len(self._vertex_list) > 0:
            s += '\nVertices:\n'\
                + '\n'.join(map(lambda x: vertex_str(x,True),self._vertex_list))
        if len(self._edge_list) > 0:
            s += '\nEdges:\n'\
                + '\n'.join(map(lambda x: edge_str(x, True), self._edge_list))
        return s

    def build(self, graph_name, overwrite=False):
        return build(graph_name,
                     self._source,
                     self._vertex_list,
                     self._edge_list,
                     is_directed=True,
                     overwrite=overwrite)


def build(graph_name, source, vertex_list, edge_list, is_directed, overwrite):
    # todo: implement column validation

    dst_hbase_table_name = generate_titan_table_name(graph_name, source)
    src_hbase_table_name = get_table_name_from_source(source)

    # Must register now to make sure the dest table is clean before calling GB
    hbase_registry.register(graph_name,
                            dst_hbase_table_name,
                            overwrite=overwrite,
                            delete_table=True)
    gb_conf_file = titan_config.write_gb_cfg(dst_hbase_table_name)
    build_command = get_gb_build_command(
        gb_conf_file,
        src_hbase_table_name,
        vertex_list,
        edge_list,
        is_directed)
    gb_cmd = ' '.join(map(str, build_command))
    logger.debug(gb_cmd)
    # print gb_cmd
    try:
        call(gb_cmd, shell=True, report_strategy=ProgressReportStrategy())
    except:
        try:  # try to clean up registry
            hbase_registry.unregister_key(graph_name, delete_table=True)
        except:
            logger.error("Graph Builder call failed and unable to unregister "
                         + "table for graph " + graph_name)
        raise

    titan_config.rexster_xml_add_graph(dst_hbase_table_name)

    return titan_graph_builder_factory.get_graph(graph_name)


def generate_titan_table_name(prefix, source):
    source_table_name = get_table_name_from_source(source)
    return '_'.join([prefix, source_table_name, "titan"])


def get_table_name_from_source(source):
    try:
        return source._table.table_name  # most likely a BigDataFrame
    except:
        # so what did we get?
        raise Exception("Could not get table name from source")


def vertex_str(vertex, public=False):
    """
    Gets the string for the vertex to use in the command call to graph_builder.
    """
    column_family = global_config['hbase_column_family']
    s = (column_family + vertex.key) if public is False else vertex.key
    if len(vertex.properties) > 0:
        s += '=' + ','.join(
            (map(lambda p: column_family + p, vertex.properties))
            if public is False else vertex.properties)
    return s


def edge_str(edge, public=False):
    """
    Gets the string for the edge to use in the command call to graph_builder.
    """
    column_family = global_config['hbase_column_family']
    s = ("{0}{1},{0}{2},{3}" if public is False else "{1},{2},{3}") \
        .format(column_family, edge.source, edge.target, edge.label)
    if len(edge.properties) > 0:
        s += ',' + ','.join((map(lambda p: column_family + p, edge.properties))
                            if public is False else edge.properties)
    return s


# static templates and commands, validated against config on load

def get_gb_build_command(gb_conf_file, table_name, vertex_list, edge_list,
                         is_directed):
    return ['hadoop',
            'jar',
            global_config['graph_builder_jar'],
            global_config['graph_builder_class'],
            '-conf',
            gb_conf_file,
            '-t',
            table_name,
            '-v',
            ', '.join(map(lambda v: '"' + vertex_str(v) + '"', vertex_list)),
            '-d' if is_directed is True else '-e',
            ', '.join(map(lambda e: '"' + edge_str(e) + '"', edge_list))
            ]

# validate the config can supply the necessary parameters
missing = []
try:
    get_gb_build_command('', '', [], [], False)
except KeyError as e:
    missing.append(str(e))

try:
    titan_config.get_rexster_server_uri('')
except KeyError as e:
    missing.append(str(e))

if len(missing) > 0:
    import sys
    sys.stderr.write("""
WARNING - Global Configuration is missing parameters for graph functionality:
  {0}

Many graph operations will fail.  Two options:

  1. edit file {1} and then do >>> global_config.load()

  2. enter values dynamically, ex.
         >>> global_config['missing_key'] = 'missing_value'
""".format(', '.join(missing), global_config.srcfile))
    sys.stderr.flush()
