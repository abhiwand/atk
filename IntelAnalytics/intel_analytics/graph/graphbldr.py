"""
Common Graph Builder classes.
"""

import abc
from intel_analytics.config import global_config, dynamic_import


__all__ = ['get_graphbuilder',
           'get_graph',
           'GraphTypes',
           'GraphTypes.Bipartite',
           'GraphTypes.Property'
           ]

class GraphTypes:
    """
    The collection of classes which represent the supported graph builder types.
    """
    class Bipartite:
        """
        Represents Bipartite Graphs.
        """
        # todo: write a better description
        pass
    class Property:
        """
        Represents Property Graphs.
        """
        # todo: write a better description
        pass

class GraphBuilderFactory(object):
    """
    Abstract class for the various graph build factories (in other words, one for Titan).
    """
    __metaclass__ = abc.ABCMeta

    #todo: implement when builder discrimination is required
    def __init__(self):
        pass

    @abc.abstractmethod
    def get_graphbuilder(self, graph_type, source):
        raise Exception("Not overridden")

    @abc.abstractmethod
    def get_graph(self, graph_name):
        raise Exception("Not overridden")



class GraphBuilder(object):
    """
    Abstract class for the various graph builders to inherit
    (not to be confused with the Tribeca "GraphBuilder" product or component).
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, source=None):
        self.register_source(source)
        pass

    def register_source(self, source):
        self._source = source

    @abc.abstractmethod
    def build(self, graph_name):
        pass

class BipartiteGraphBuilder(GraphBuilder):
    """
    Abstract class for py bipartite graph builders.
    """
    __metaclass__ = abc.ABCMeta


    def __init__(self, source=None):
        super(BipartiteGraphBuilder, self).__init__(source)
        self._vertex_list = []

    @abc.abstractmethod
    def build(self, graph_name):
        pass

    def register_vertex(self, key, properties=None):
        """
        register_vertex('id')
        register_vertex('id', ['name', 'age', 'dept'])
        """
        if len(self._vertex_list) > 2:
            raise ValueError(
                "ERROR: Attempt to register more than two vertex sources " + \
                "for a bipartite graph; check vertex registration or switch" + \
                "to a property graph builder")
        self._vertex_list.append(GraphBuilderVertex(key, properties))

    def register_vertices(self, vertices):
        """
        Parameters:
        vertices: List of (id and list of properties)'s.
        Example: register_vertices([('id', ['name', 'age', 'dept']),
                                    ('manager', ['income', 'org'])])
        """
        for entry in vertices:
            if len(entry) != 2:
                raise ValueError("ERROR: Incorrect vertex tuple: " + str(entry))
            self.register_vertex(entry[0], entry[1])


class PropertyGraphBuilder(GraphBuilder):
    """
    An abstract class for py property graph builders.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, source=None):
        super(PropertyGraphBuilder, self).__init__(source)
        self._vertex_list = []
        self._edge_list = []

    @abc.abstractmethod
    def build(self, graph_name):
        pass

    def register_vertex(self, key, properties=None):
        """
        register_vertex('id')
        register_vertex('id', ['name', 'age', 'dept'])
        """
        self._vertex_list.append(GraphBuilderVertex(key, properties))

    def register_vertices(self, vertices):
        """
        Parameters:
        vertices: List of (key and list of properties)'s
        Example: register_vertices([('id', ['name', 'age', 'dept']),
                                    ('manager', ['income', 'org'])])
        """
        for entry in vertices:
            if len(entry) != 2:
                raise ValueError("ERROR: Incorrect vertex tuple: " + str(entry))
            self.register_vertex(entry[0], entry[1])

    def register_edge(self, edge_tuple, properties=None):
        """
        Parameters:
        edge: Tuple of source, target and label
        properties: List of property sources
        Example: register_edge(('src', 'tgt', 'label'), ['ep1', 'ep2'])
        """
        self._edge_list.append(GraphBuilderEdge(edge_tuple, properties))

    def register_edges(self, edges):
        """
        Parameters:
        edges: List of tuples of tuples and lists
        Example:
         register_edges([(('src', 'tgt', 'label'), ['ep1', 'ep2']), (), ()])
        """
        for edge in edges:
            if len(edge) != 2:
                raise ValueError("ERROR: Incorrect edge tuple: " + str(edge))
            self.register_edge(edge[0], edge[1])


class GraphBuilderVertex:
    """
    An entry for GraphBuilder vertex registration.
    """
    def __init__(self, key, properties=None):
        if key is None: raise Exception("Invalid key_src: None")
        if properties is None: properties = []
        self.key = key
        self.properties = properties

    def __repr__(self):
        "'{0}', [{1}]".format(self.key,
                             ("'" + "', '".join(self.properties) + "'")
                              if len(self.properties) > 0 else "")

    def register_property(self, vertex_property):
        self.properties.append(vertex_property)

    def get_properties(self):
        return self.properties

    def is_duplicate_property(self, vertex_property):
        return True if vertex_property in self.properties else False

    def set_filter(self, vertex_filter):
        self.filter = vertex_filter

class GraphBuilderEdge:
    """
    An entry for GraphBuilder edge registration.
    """
    def __init__(self, edge_tuple, properties=None):
        if len(edge_tuple) != 3:
            raise ValueError("ERROR: Incorrect edge tuple. Edges are" +
                             "defined as (source, target, label)")
        (self.source, self.target, self.label) = edge_tuple
        if self.source is None: raise Exception("Invalid edge source: None")
        if self.target is None: raise Exception("Invalid edge target: None")
        if self.label is None: raise Exception("Invalid edge label: None")
        if properties is None: properties = []
        self.properties = properties

    def __repr__(self):
        "('{0}','{1}','{2}'), [{3}]".format(
                            self.source,
                            self.target,
                            self.label,
                            ("'" + "', '".join(self.properties) + "'")
                            if len(self.properties) > 0 else "")

    def register_property(self, edge_property):
        self.properties.append(edge_property)

    def get_properties(self):
        return self.properties

    def is_duplicate_property(self, edge_property):
        return True if edge_property in self.properties else False

    def set_filter(self, edge_filter):
        self.filter = edge_filter

    def has_label(self):
        return bool(self.label)


def get_graphbuilder(graph_type, source=None):
    """
    Returns a graphbuilder for a given graph type.

    Parameters
    ----------
    graph_type : GraphTypes.*
        A class indicating the type of graph, such as GraphTypes.Property
        or GraphTypes.Bipartite.
    """
    factory_class = _get_graphbuilder_factory_class()
    return factory_class.get_graphbuilder(graph_type, source)

def get_graph(graph_name):
    factory_class = _get_graphbuilder_factory_class()
    return factory_class.get_graph(graph_name)


# dynamically and lazily load the correct graphbuilder factory,
# according to config
graphbuilder_factory = None
def _get_graphbuilder_factory_class():
    global graphbuilder_factory
    if graphbuilder_factory is None:
        graphbuilder_factory_class = dynamic_import(
            global_config['py_graphbuilder_factory_class'])
        graphbuilder_factory = graphbuilder_factory_class.get_instance()
    return graphbuilder_factory

