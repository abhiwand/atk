"""
Titan-specific graph implementation
"""

__all__ = []

from intel_analytics.graph.biggraph import \
    PropertyGraphBuilder, BipartiteGraphBuilder,\
    GraphBuilderEdge, GraphBuilderFactory, GraphTypes

from intel_analytics.graph.titan.ml import TitanGiraphMachineLearning
from intel_analytics.graph.titan.config import titan_config
from intel_analytics.subproc import call
from intel_analytics.config import global_config

from bulbs.titan import Graph as bulbsGraph
from bulbs.config import DEBUG
from bulbs.config import Config as bulbsConfig

import json
import os

#class TitanGraph(object):   # TODO: inherit BigGraph later
#    """
#    proxy for a graph stored in Titan
#    """
#    def __init__(self):
#        self.ml = TitanGiraphMachineLearning(self)


class TitanNameRegistry(object):
    """
    Maintains map of user-given names to titan-hbase names, persisted to a file
    """
    def __init__(self):
        self._load_map()

    def register_graph_name(self, graph_name, titan_table_name):
        self._titan_table_names[graph_name] = titan_table_name
        self._persist_map()
        pass

    def get_graph_names(self):
        return self._titan_table_names.keys()

    def get_titan_table_name(self, graph_name):
        try:
            return self._titan_table_names[graph_name]
        except KeyError:
            raise KeyError("Could not find titan table name for graph '"
                           + graph_name + "'")

    def get_graph_name(self, titan_table_name):
        try:
            return (key for key,value in self._titan_table_names.items()
                    if value == titan_table_name).next()
        except StopIteration:
            raise ValueError("Could not find graph name to titan table '"
                             + titan_table_name + "'")

    # todo: make persist_map and load_map thread-safe
    def _persist_map(self):
        dstpath = global_config['titan_conf_folder']
        if not os.path.exists(dstpath):
            os.makedirs(dstpath)
        dstfile = os.path.join(dstpath, global_config['titan_names_file'])
        try:
            with os.fdopen(os.open(dstfile, os.O_WRONLY | os.O_CREAT,
                                   int("0777", 8)), 'w') as dst:
                json.dump(self._titan_table_names, dst)
        except IOError:
            #todo: log...
            raise Exception("Could not open graph names file for writing.  " +
                            "Check if path exists: " + dstfile)

    def _load_map(self):
        srcfile = os.path.join(global_config['titan_conf_folder'],
                               global_config['titan_names_file'])
        try:
            with open(srcfile, 'r') as src:
                self._titan_table_names = json.load(src)
        except IOError:
            #todo: log...
            self._titan_table_names = {}
            self._persist_map()


#-----------------------------------------------------------------------------
# HBase to Titan graph building
#-----------------------------------------------------------------------------
class TitanGraphBuilderFactory(GraphBuilderFactory):
    """
    Provides a set of Titan graph builders
    """
    def __init__(self):
        super(TitanGraphBuilderFactory, self).__init__()
        self._active_titan_table_name = None
        self._name_registry = TitanNameRegistry()

    def get_graph_builder(self, graph_type, source=None):
        if graph_type is GraphTypes.Bipartite:
            return HBase2TitanBipartiteGraphBuilder(source)
        elif graph_type is GraphTypes.Property:
            return HBase2TitanPropertyGraphBuilder(source)
        else:
            raise Exception("Unsupported graph type: " + str(graph_type))

    def get_graph(self, graph_name):
        titan_table_name = self._name_registry.get_titan_table_name(graph_name)
        return self._get_graph(graph_name, titan_table_name)

    def get_graph_names(self):
        return self._name_registry.get_graph_names()

    def activate_graph(self, graph):
        self._activate_titan_table(graph.titan_table_name)
        pass

    def get_active_graph_name(self):
        if self._active_titan_table_name is None:
            return ""
        return self._name_registry.get_graph_name(self._active_titan_table_name)

    def _activate_titan_table(self, titan_table_name):
        """changes rexster's configuration to point to given graph
        """
        # write new cfg file for rexster to turn its attention to the new graph
        try:
            titan_config.write_rexster_cfg(titan_table_name)
        except ValueError:
            raise ValueError('ERROR: Failed to reconfigure rexster server')
        self._active_titan_table_name = titan_table_name

    def _get_graph(self, graph_name, titan_table_name):
        self._activate_titan_table(titan_table_name)
        rexster_server_uri = get_rexster_server_uri(titan_table_name)
        bulbs_config = bulbsConfig(rexster_server_uri)
        bulbs_config.set_logger(DEBUG)
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
    bipartite graph builder for HBase->Titan
    """
    def __init__(self, source=None):
        super(HBase2TitanBipartiteGraphBuilder, self).__init__(source)

    def __repr__(self):
        s = "Source: " \
            + (str(self._source) if self._source is not None else "None")
        if len(self._vertex_list) > 0:
            s += '\nVertices:\n' + \
                '\n'.join(map(lambda x: vertex_str(x, True), self._vertex_list))
        return s

    def build(self, graph_name):
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
                     is_directed=False)


class HBase2TitanPropertyGraphBuilder(PropertyGraphBuilder):
    """
    property graph builder for HBase->Titan
    """
    def __init__(self, source=None):
        super(HBase2TitanPropertyGraphBuilder, self).__init__(source)

    def __repr__(self):
        s = "Source: "\
            + (str(self._source) if self._source is not None else "None")
        if len(self._vertex_list) > 0:
            s += '\nVertices:\n'\
                + '\n'.join(map(lambda x: vertex_str(x,True), self._vertex_list))
        if len(self._edge_list) > 0:
            s += '\nEdges\n'\
                + '\n'.join(map(lambda x: edge_str(x,True), self._edge_list))
        return s

    def build(self, graph_name):
        return build(graph_name,
                     self._source,
                     self._vertex_list,
                     self._edge_list,
                     is_directed=True)


def build(graph_name, source, vertex_list, edge_list, is_directed):
    # validate column sources
    # todo: implement column validation
    if source is None:
        raise Exception("Graph has no source. Try register_source()")

    # build
    titan_table_name = generate_titan_table_name(graph_name, source)
    hbase_table_name = get_table_name_from_source(source)
    gb_conf_file = titan_config.write_gb_cfg(titan_table_name)
    build_command = get_gb_build_command(
        gb_conf_file,
        hbase_table_name,
        vertex_list,
        edge_list,
        is_directed)
    call(build_command)

    titan_graph_builder_factory._name_registry.\
        register_graph_name(graph_name, titan_table_name)

    return titan_graph_builder_factory.get_graph(graph_name)


def generate_titan_table_name(prefix, source):
    source_table_name = get_table_name_from_source(source)
    return prefix + "_" + source_table_name


def get_table_name_from_source(source):
    try:
        return source._table.table_name  # most likely a BigDataFrame
    except:
        # so what did we get?
        raise Exception("Could not get table name from source")


def vertex_str(vertex, public=False):
    """
    get string for vertex to use in command call to graph_builder
    """
    column_family = global_config['hbase_column_family']
    s = (column_family + vertex.key) if public is False else vertex.key
    if len(vertex.properties) > 0:
        s += '=' + ','.join(
            (map(lambda p: column_family + ':' + p, vertex.properties))
            if public is False else vertex.properties)
    return s


def edge_str(edge, public=False):
    """
    get string for edge to use in command call to graph_builder
    """
    column_family = global_config['hbase_column_family']
    s = ("{0}{1},{0}{2},{3}" if public is False else "{1},{2},{3}") \
            .format(column_family, edge.source, edge.target, edge.label)
    if len(edge.properties) > 0:
        s += '=' + ','.join(
            (map(lambda p: column_family + ':' + p, edge.properties))
            if public is False else edge.properties)
    return s


# static templates and commands, validated against config on load

from string import Template


def get_template(config, template_str):
    template = Template(template_str)
    config.verify_template(template)
    return template


def fill_template(template, config, args):
    return template.substitute(config).format(*args)


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
            '"' + ' '.join(map(lambda v: vertex_str(v), vertex_list)) + '"',
            '-d' if is_directed is True else '-e',
            '"' + ' '.join(map(lambda e: edge_str(e), edge_list)) + '"']


def get_rexster_server_uri(table_name):
    return '{0}:{1}/graphs/{2}'.format(
        global_config['rexster_baseuri'],
        global_config['rexster_bulbs_port'],
        table_name)

# validate the config can supply the necessary parameters
missing = []
try:
    get_gb_build_command('', '', [], [], False)
except KeyError as e:
    missing.append(str(e))

try:
    get_rexster_server_uri('')
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
