"""
Common Graph Builder classes
"""

import abc


class GraphTypes:
    """
    Collection of classes which represent the supported graph builder types
    """
    class Bipartite:
        """
        Bipartite Graph
        """
        # todo: write a better description
        pass
    class Property:
        """
        Property Graph
        """
        # todo: write a better description
        pass

class GraphBuilderFactory(object):
    """
    Abstract class for the various graph build factories (i.e. one for Titan)
    """
    #todo: implement when builder discrimination is required
    def __init__(self):
        pass

    @abc.abstractmethod
    def get_graphbuilder(self, graph_type, table):
        pass



class GraphBuilder(object):
    """
    Abstract class for the various graph builders to inherit
    (not to be confused by the Tribeca "GraphBuilder" product or component)
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, table):
        self._table = table

    @abc.abstractmethod
    def build(self):
        pass

class BipartiteGraphBuilder(GraphBuilder):
    """
    Abstract class for py bipartite graph builders
    """
    __metaclass__ = abc.ABCMeta


    def __init__(self, table):
        super(BipartiteGraphBuilder, self).__init__(table)
        self.vertex_list = []

    @abc.abstractmethod
    def build(self):
        pass

    def register_vertex(self, key, properties):
        """
        register_vertex('id')
        register_vertex('id', ['name', 'age', 'dept'])
        """
        if len(self.vertex_list) > 2:
            raise ValueError(
                "ERROR: Attempt to register more than two vertex sources " + \
                "for a bipartite graph; check vertex registration or switch" + \
                "to a property graph builder")
        self.vertex_list.append(GraphBuilderVertex(key, properties))

    def register_vertices(self, vertices):
        """
        Parameters:
        vertices: List of (id and list of properties)'s
        Example: register_vertices([('id', ['name', 'age', 'dept']),
                                    ('manager', ['income', 'org'])])
        """
        for entry in vertices:
            if len(entry) != 2:
                raise ValueError("ERROR: Incorrect vertex tuple: " + str(entry))
            self.register_vertex(entry[0], entry[1])

class PropertyGraphBuilder(GraphBuilder):
    """
    Abstract class for py property graph builders
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, table):
        super(PropertyGraphBuilder, self).__init__(table)
        self.vertex_list = []
        self.edge_list = []

    @abc.abstractmethod
    def build(self):
        pass

    def register_vertex(self, key, properties):
        """
        register_vertex('id')
        register_vertex('id', ['name', 'age', 'dept'])
        """
        self.vertex_list.append(GraphBuilderVertex(key, properties))

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

    def register_edge(self, edge_tuple, properties):
        """
        Parameters:
        edge: Tuple of source, target and label
        properties: List of property sources
        Example: register_edge(('src', 'tgt', 'label'), ['ep1', 'ep2'])
        """
        self.edge_list.append(GraphBuilderEdge(edge_tuple, properties))

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
    An entry for GraphBuilder vertex registration
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
    An entry for GraphBuilder edge registration
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

