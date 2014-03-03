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
from intel_analytics.pig import get_pig_args_with_gb

__all__ = ['BulbsGraphWrapper']

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
from intel_analytics.logger import stdout_logger as logger
from xml.etree.ElementTree import tostring
import xml.etree.cElementTree as ET
from intel_analytics.report import FaunusProgressReportStrategy

try:
    from intel_analytics.pigprogressreportstrategy import PigProgressReportStrategy as etl_report_strategy#depends on ipython
except ImportError, e:
    from intel_analytics.report import PrintReportStrategy as etl_report_strategy, FaunusProgressReportStrategy


#class TitanGraph(object):   # TODO: inherit BigGraph later
#    """
#    The proxy for a graph stored in Titan.
#    """
#    def __init__(self):
#        self.ml = TitanGiraphMachineLearning(self)


#-----------------------------------------------------------------------------
# HBase to Titan graph building
#-----------------------------------------------------------------------------
class TitanGraphBuilderFactory(GraphBuilderFactory):
    """
    This class provides a set of Titan graph builders.
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
        titan_graph = BulbsGraphWrapper(bulbsGraph(bulbs_config))
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
    The bipartite graph builder for HBase to Titan.
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

    def build(self, graph_name, overwrite=False, append=False, flatten=False):
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
                     overwrite=overwrite,
                     append=append,
                     flatten=flatten)


class HBase2TitanPropertyGraphBuilder(PropertyGraphBuilder):
    """
    The property graph builder for HBase to Titan.
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

    def build(self, graph_name, overwrite=False, append=False, flatten=False):
        return build(graph_name,
                     self._source,
                     self._vertex_list,
                     self._edge_list,
                     is_directed=True,
                     overwrite=overwrite,
                     append=append,
                     flatten=flatten)


def build(graph_name, source, vertex_list, edge_list, is_directed, overwrite, append, flatten):

    # TODO: implement column validation

    dst_hbase_table_name = generate_titan_table_name(graph_name, source)
    src_hbase_table_name = _get_table_name_from_source(source)

    # TODO: Graph Builder could handle overwrite instead of the registry, not sure if that is better?

    # Must register now to make sure the dest table is clean before calling GB
    hbase_registry.register(graph_name,
                            dst_hbase_table_name,
                            overwrite=overwrite,
                            append=append,
                            delete_table=not append)

    gb_conf_file = titan_config.write_gb_cfg(dst_hbase_table_name)

    cmd = get_gb_build_command(gb_conf_file, src_hbase_table_name, vertex_list, edge_list, is_directed, overwrite,
                               append, flatten)

    return_code = call(cmd, report_strategy=etl_report_strategy())

    if return_code:
        try:  # try to clean up registry
            hbase_registry.unregister_key(graph_name, delete_table=not append)
        except:
            logger.error("Graph Builder call failed and unable to unregister "
                              + "table for graph " + graph_name)
        raise Exception('Could not load titan')

    titan_config.rexster_xml_add_graph(dst_hbase_table_name)

    return titan_graph_builder_factory.get_graph(graph_name)


def generate_titan_table_name(prefix, source):
    source_table_name = _get_table_name_from_source(source)
    return '_'.join([prefix, source_table_name, "titan"])


def _get_table_name_from_source(source):
    try:
        return source._table.table_name  # Most likely a BigDataFrame.
    except:
        # So what did we get?
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


def get_gb_build_command(gb_conf_file, table_name, vertex_list, edge_list, is_directed, overwrite, append, flatten):
    """
    Build the Pig command line call to the Jython script
    """
    args = get_pig_args_with_gb('pig_load_titan.py')

    # These are the command line arguments are for the Jython script.  It is confusing
    # because they aren't the same as the command line args that GraphBuilder takes
    args += ['-t', table_name,
             '-c', gb_conf_file,
             # edge and vertices lists should be surrounded with double quotes
             '-e', ' '.join(map(lambda e: '"' + edge_str(e) + '"', edge_list)),
             '-v', ' '.join(map(lambda v: '"' + vertex_str(v) + '"', vertex_list))]

    # TODO: can we pass argument names without a value? These are all on/off flags only
    if append:
        args += ['-a', 'is_append']
    if overwrite:
        args += ['-o', 'is_overwrite']
    if is_directed:
        args += ['-d', 'is_directed']
    if flatten:
        args += ['-f', 'is_flatten']

    return args


# validate the config can supply the necessary parameters
missing = []
try:
    # needed by Jython script passed to Pig
    global_config['graph_builder_jar']
    global_config['graph_builder_pig_udf']
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


class BulbsGraphWrapper:
    def __init__(self, graph):
        self._graph = graph
        self._graph.vertices.remove_properties = lambda n : self.__raise_(Exception('The feature is not currently supported'))
        self._graph.edges.remove_properties = lambda n : self.__raise_(Exception('The feature is not currently supported'))
        self.client_class = graph.client_class
        self.default_index = graph.default_index

    @property
    def vertices(self):
        return self._graph.vertices

    @property
    def edges(self):
        return self._graph.edges

    @property
    def client(self):
        return self._graph.client

    @property
    def config(self):
        return self._graph.config

    @property
    def factory(self):
        return self._graph.factory

    @property
    def gremlin(self):
        return self._graph.gremlin

    @property
    def scripts(self):
        return self._graph.scripts

    def load_graphml(self,uri):
        """
        Loads a GraphML file into the database and returns the response.

        Parameters
        ----------
        uri : str
        URI of the GraphML file to load.

        Returns
        -------
        RexsterResult

        Examples
        --------
        >>> graph.load_graphml('file:///home/user/graphml_dir/demo.xml')

        """
        return self._graph.load_graphml(uri)

    def get_graphml(self):
        """
        Returns a GraphML file representing the entire database.

        Returns
        -------
        RexsterResult

        Examples
        --------
        >>> graph = get_graph("SampleGraph")
        >>> result = graph.get_graphml()

        """
        return self._graph.get_graphml()


    def clear(self):
        """
        Deletes all the elements in the graph.

        Returns
        -------
        RexsterResult

        Examples
        --------
        >>> graph = get_graph("SampleGraph")
        >>> graph.clear()
        """
        return self._graph.clear()

    def export_as_graphml(self, statements, file):
        """
        Execute graph queries and output result as a graphml file in the specified file location.

        Parameters
        ----------
        statements : Iterable
           Iterable of query strings. The query returns vertices or edges.
           For example, g.V('name','user_123').out
        file: String
            output file path

        Examples
        --------
        >>> statements = []
        >>> statements.append("g.V('name','user_123').out")
        >>> graph = get_graph("SampleGraph")
        >>> graph.export_as_graphml(statements, "example.xml")

        """
        xml = '\"' + self._get_query_xml(statements) + '\"'
        temp_output = 'graph_query'

        args = []
        args += ['hadoop',
                 'jar',
                 global_config['intel_analytics_jar'],
                 global_config['graphml_exporter_class'],
                 '-f', file,
                 '-o', temp_output,
                 '-q', xml,
                 '-t', self.titan_table_name
        ]

        return_code = call(args, report_strategy=FaunusProgressReportStrategy())
        if return_code:
            raise Exception('Could not export graph')

    def _get_query_xml(self, statements):
        """
        Returns a xml containing query statement as individual child node

        Parameters
        ----------
        statements : Iterable
            Iterable of query strings

        Returns
        -------
        query xml string
        """
        root = ET.Element("query")

        for statement in statements:
            statementNode = ET.SubElement(root, "statement")
            statementNode.text = statement + ".transform('{[it,it.map()]}')"

        return tostring(root)


    def __raise_(self, ex):
        raise ex
