"""
Titan-specific graph implementation
"""

__all__ = []

from intel_analytics.graph.graphbldr import \
    PropertyGraphBuilder, BipartiteGraphBuilder,\
    GraphBuilderEdge, GraphBuilderFactory, GraphTypes

from intel_analytics.graph.titan.ml import TitanGiraphMachineLearning
from intel_analytics.graph.titan.config import titan_config
from intel_analytics.subproc import call
from intel_analytics.config import global_config

from bulbs.titan import Graph as bulbsGraph
from bulbs.config import DEBUG
from bulbs.config import Config as bulbsConfig

#class TitanGraph(object):   # TODO: inherit BigGraph
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
    Provides a set of graph builders for a particular table
    """
    def get_graphbuilder(graph_type, table):
        if graph_type is GraphTypes.Bipartite:
            return HBase2TitanBipartiteGraphBuilder(table)
        elif graph_type is GraphTypes.Property:
            return HBase2TitanPropertyGraphBuilder(table)
        else:
            raise Exception("Unsupported graph type: " + str(graph_type))

class HBase2TitanBipartiteGraphBuilder(BipartiteGraphBuilder):
    """
    bipartite graph builder for HBase->Titan
    """
    def __init__(self, table):
        super(HBase2TitanBipartiteGraphBuilder, self).__init__(table)

    def build(self):
        if len(self.vertex_list) != 2:
            raise ValueError("ERROR: bipartite graph construction requires " +
                "2 vertex sources; " + str(len(self.vertex_list)) + " detected")

        # create the one edge source for bipartite
        edge_list = [GraphBuilderEdge(
            (self.vertex_list[0].key, self.vertex_list[1].key, '_no_label'))]

        return build(self._table.table_name,
                     self.vertex_list,
                     edge_list,
                     is_directed=False)


class HBase2TitanPropertyGraphBuilder(PropertyGraphBuilder):
    """
    property graph builder for HBase->Titan
    """
    def __init__(self, table):
        super(HBase2TitanPropertyGraphBuilder, self).__init__(table)

    def build(self):
        return build(self._table.table_name,
                     self.vertex_list,
                     self.edge_list,
                     is_directed=True)


def build(table_name, vertex_list, edge_list, is_directed):
    # validate column sources
    # todo: implement column validation

    # build
    gb_conf_file = titan_config.write_gb_cfg(table_name)
    build_command = get_gb_build_command(
        gb_conf_file,
        table_name,
        vertex_list,
        edge_list,
        is_directed)
    call(build_command)

    # retrieve
    try:
        # restart rexster
        rexster_kill_command = get_rexster_kill_command()
        call(rexster_kill_command)
        rexster_cfg_file = titan_config.write_rexster_cfg(table_name)
        rexster_start_command = get_rexster_start_command(rexster_cfg_file)
        call(rexster_start_command)
    except ValueError:
        raise ValueError('ERROR: Failed to restart rexster server')

    rexster_server_uri = get_rexster_server_uri(table_name)
    bulbs_config = bulbsConfig(rexster_server_uri)
    bulbs_config.set_logger(DEBUG)
    titan_graph = bulbsGraph(bulbs_config)
    titan_graph.ml = TitanGiraphMachineLearning(titan_graph)
    return titan_graph

def vertex_str(vertex):
    """
    get string for vertex to use in command call to graphbuilder
    """
    column_family = global_config['hbase_column_family']
    s = column_family + ':' + vertex.name
    if len(vertex.properties) > 0:
        s += '=' + ','.join(
            map(lambda p : column_family + ':' + p, vertex.properties))
    return s

def edge_str(edge):
    """
    get string for edge to use in command call to graphbuilder
    """
    column_family = global_config['hbase_column_family']
    s = "{0}:{1},{0}:{2},{3}" \
            .format(column_family, edge.source, edge.target, edge.label)
    if len(edge.properties) > 0:
        s += '=' + \
            ','.join(map(lambda p : column_family + ':' + p, edge.properties))
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
    return [global_config['hadoop'],
            'jar',
            global_config['graphbuilder_jar'],
            global_config['graphbuilder_class'],
            '-conf',
            gb_conf_file,
            '-t',
            table_name,
            '-v',
            '"' + ' '.join(map(lambda v : vertex_str(v), vertex_list)) + '"',
            '-d' if is_directed is True else '-e',
            '"' + ' '.join(map(lambda e : edge_str(e), edge_list)) + '"']

def get_rexster_server_uri(table_name):
    return '{0}:{1}/graphs/{2}'.format(
        global_config['rexster_baseuri'],
        global_config['rexster_bulbs_port'],
        table_name)

def get_rexster_start_command(rexster_config_file):
    return [
        global_config['rexster_start_script'],
        '-s',
        '-c',
        rexster_config_file,
        '&'  # not sure that will fly?
    ]

def get_rexster_kill_command():
    return [
        global_config['rexster_kill_script']
    ]

# validate the config can supply the necessary parameters
missing = []

try: get_gb_build_command('', '', [], [], False)
except KeyError as e: missing.append(str(e))

try: get_rexster_server_uri('')
except KeyError as e: missing.append(str(e))

try: get_rexster_start_command('')
except KeyError as e: missing.append(str(e))

try: get_rexster_kill_command()
except KeyError as e: missing.append(str(e))

if len(missing) > 0:
    global_config.raise_missing_parameters_error(missing)

