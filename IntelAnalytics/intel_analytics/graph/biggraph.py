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
The common methods and classes for building big graphs
"""

import abc
from intel_analytics.config import global_config, dynamic_import


__all__ = ['get_graph_builder',
           'get_graph',
           'get_graph_names',
           'BipartiteGraphBuilder',
           'PropertyGraphBuilder',
#           'GraphTypes',
#           'BigGraph',
           ]

my_frame = None

class BigGraph(object):   # note: not using this for 0.5
    pass


class GraphTypes:
    """
    A collection of classes which represent the supported graph builder types.
    """
    class Bipartite:
        """
        The Bipartite Graph class.
        """
        # todo: write a better description
        pass

    class Property:
        """
        The Property Graph class.
        """
        # todo: write a better description
        pass


class GraphBuilderFactory(object):
    """
    An abstract class for the various graph build factories 
	(for example, one for Titan).
    """
    __metaclass__ = abc.ABCMeta

    #todo: implement when builder discrimination is required
    def __init__(self):
        pass

    @abc.abstractmethod
    def get_graph_builder(self, graph_type, source):
        """
        Returns a new graph builder

        Gets a graph builder which will build the given type of graph for the
        given source

        Parameters
        ----------
        graph_type : (GraphTypes.Property | GraphTypes.Bipartite )
            the type of graph to create.
        source : BigDataFrame
            the source of the data for the graph to build

        Returns
        -------
        graph_builder : GraphBuilder
            new graph builder object
        """
        raise Exception("Not overridden")

    @abc.abstractmethod
    def get_graph(self, graph_name):
        """
        Returns a graph object for a previously created graph

        Parameters
        ----------
        graph_name : string
            user-given name of a previously created graph

        Returns
        -------
        graph : Graph
            new graph object
        """
        raise Exception("Not overridden")

    @abc.abstractmethod
    def get_graph_names(self):
        """
        Returns a list of names of all the previously created graphs on record.
        """
        raise Exception("Not overridden")


class GraphBuilder(object):
    """
    An abstract class for the various graph builders to inherit
    (not to be confused with the Tribeca "GraphBuilder" product or component).
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, source):
        self._source = source

    @abc.abstractmethod
    def build(self, graph_name, overwrite, append, flatten, retainDanglingEdges):
        """
        Builds a graph according to the settings in the builder.

        Overwrite and append are mutually exclusive (you can't overwrite and
        append to a graph at the same time).

        Parameters
        ----------
        graph_name : string
            name for the new graph
        overwrite : Bool, optional
            if the given graph_name already exists, overwrite=True will
            overwrite the existing graph; overwrite=False will raise an Error
        append : Bool, optional
            if the given graph_name already exists, append=True will update
            existing graph elements and create ones that do not already exist.
        flatten : Bool, optional
            specifies that when a cell containing a JSon list is read
            as a vertex ID, it is to be expanded into one vertex for each
            entry in the list. This applies to the source and destination
            columns for edges as well. It does not apply to properties.
        retainDanglingEdges : Bool, optional
            retains the dangling edges with missing source or target vertices
        Returns
        -------
        graph : Graph
            new graph object
        """
        pass


class BipartiteGraphBuilder(GraphBuilder):
    """
    Graph Builder which builds bipartite graphs
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, source=None):
        super(BipartiteGraphBuilder, self).__init__(source)
        self._vertex_list = []
        self.registered_vertex_properties = None # vertex properties registered with register_vertex_properties()
        self.registered_edge_properties = None # edge properties registered with register_edge_properties()

    @abc.abstractmethod
    def build(self, graph_name, overwrite=False, append=False, flatten=False, retainDanglingEdges=False, withSideVertexProperty=False):
        """
        Builds a bipartite graph according to the settings in the builder.

        Overwrite and append are mutually exclusive (you can't overwrite and
        append to a graph at the same time).

        Parameters
        ----------
        graph_name : string
            name for the new graph
        overwrite : Bool, optional
            if the given graph_name already exists, overwrite=True will
            overwrite the existing graph; overwrite=False will raise an Error
        append : Bool, optional
            if the given graph_name already exists, append=True will update
            existing graph elements and create ones that do not already exist.
        flatten : Bool, optional
            specifies that when a cell containing a JSon list is read
            as a vertex ID, it is to be expanded into one vertex for each
            entry in the list. This applies to the source and destination
            columns for edges as well. It does not apply to properties.
        retainDanglingEdges : Bool, optional
            retains the dangling edges with missing source or target vertices
        withSideVertexProperty : Bool, optional
            adds the 'side' property to all vertices in a bipartite-graph            

        Returns
        -------
        graph : Graph
            new graph object
        """
        pass

    def register_vertex(self, key, properties=None):
        """
        Specify vertex creation rule

        Parameters
        ----------
        key : string
            the source key for the vertex, usually a column name
        properties : list of strings, optional
            the source keys for the vertex properties, usually column names

        Examples
        --------
        >>> gb = get_graph_builder(GraphTypes.Bipartite, my_frame)
        >>> gb.register_vertex('id', ['name', 'age', 'dept'])
        """
        if len(self._vertex_list) > 2:

            raise ValueError("""
ERROR: Attempt to register more than two vertex sources for a bipartite
graph; check vertex registration or switch to a property graph builder""")

        self._vertex_list.append(GraphBuilderVertex(key, properties))
        
    def register_vertices(self, vertices):
        """
        Specify vertex creation rules (see register_vertex)

        Parameters
        ----------
        vertices : list of tuples
            List of [(key, list of properties), ...]

        Examples
        --------
        >>> gb = get_graph_builder(GraphTypes.Bipartite, my_frame)
        >>> gb.register_vertices([('id', ['name', 'age', 'dept']), ('manager', ['income', 'org'])])
        """
        for entry in vertices:
            if len(entry) != 2:
                raise ValueError("ERROR: Incorrect vertex tuple: " + str(entry))
            self.register_vertex(entry[0], entry[1])
            
    def register_vertex_properties(self, vertex_property_frame, key, vertex_property_cols):
        """
        Parameters
        ----------
        vertex_property_frame: a frame holding the additional vertex properties to register
        key: the name of the vertex id column used to match the vertices that are already registered
        vertex_property_cols: list of columns in the specified vertex_property_frame to use for vertex properties 
        
        Examples
        ----------
        >>> gb = get_graph_builder(GraphTypes.Bipartite, my_frame)
        >>> gb.register_vertex_properties(v_prop_frame, 'id', ['name', 'age', 'dept'])
        """
        self.registered_vertex_properties = GraphBuilderVertexProperties(vertex_property_frame, key, vertex_property_cols)
        
    def register_edge_properties(self, edge_property_frame, edge_tuple, edge_property_cols):
        """
        Parameters
        ----------
        edge_property_frame: a frame holding the additional edge properties to register
        edge_tuple: the tuple specifying the edge id (src_vertex_id, dest_vertex_id, edge_label) used to match the edges that are already registered
        edge_property_cols: list of columns in the specified edge_property_frame to use for edge properties 
        
        Examples
        ----------
        >>> gb = get_graph_builder(GraphTypes.Bipartite, my_frame)
        >>> gb.register_edge_properties(v_prop_frame, 'id', ['name', 'age', 'dept'])
        """
        self.registered_edge_properties = GraphBuilderEdgeProperties(edge_property_frame, edge_tuple, edge_property_cols)     

class PropertyGraphBuilder(GraphBuilder):
    """
    Graph Builder which builds property graphs
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, source=None):
        super(PropertyGraphBuilder, self).__init__(source)
        self._vertex_list = []
        self._edge_list = []
        self.registered_vertex_properties = None # vertex properties registered with register_vertex_properties()
        self.registered_edge_properties = None # edge properties registered with register_edge_properties()

    @abc.abstractmethod
    def build(self, graph_name, overwrite=False, append=False, flatten=False,
              retainDanglingEdges=False):
        """
        Builds a property graph according to the settings in the builder.

        Overwrite and append are mutually exclusive (you can't overwrite and
        append to a graph at the same time).

        Parameters
        ----------
        graph_name : string
            name for the new graph
        overwrite : Bool, optional
            if the given graph_name already exists, overwrite=True will
            overwrite the existing graph; overwrite=False will raise an Error
        append : Bool, optional
            if the given graph_name already exists, append=True will update
            existing graph elements and create ones that do not already exist.
        flatten : Bool, optional
            specifies that when a cell containing a JSon list is read
            as a vertex ID, it is to be expanded into one vertex for each
            entry in the list. This applies to the source and destination
            columns for edges as well. It does not apply to properties.
        retainDanglingEdges : Bool, optional
            retains the dangling edges with missing source or target vertices
            
        Returns
        -------
        graph : Graph
            new graph object
        """
        pass

    def register_vertex(self, key, properties=None):
        """
        Specify vertex creation rule

        Parameters
        ----------
        key : string
            the source key for the vertex, usually a column name
        properties : list of strings, optional
            the source keys for the vertex properties, usually column names

        Examples
        --------
        >>> gb = get_graph_builder(GraphTypes.Property, my_frame)
        >>> gb.register_vertex('id', ['name', 'age', 'dept'])
        """
        self._vertex_list.append(GraphBuilderVertex(key, properties))

    def register_vertices(self, vertices):
        """
        Specify vertex creation rules (see register_vertex)

        Parameters
        ----------
        vertices : list of tuples
            List of [(key, list of properties), ...]

        Examples
        --------
        >>> gb = get_graph_builder(GraphTypes.Property, my_frame)
        >>> gb.register_vertices([('id', ['name', 'age', 'dept']), ('manager', ['income', 'org'])])
        """
        for entry in vertices:
            if len(entry) != 2:
                raise ValueError("ERROR: Incorrect vertex tuple: " + str(entry))
            self.register_vertex(entry[0], entry[1])
            
    def register_vertex_properties(self, vertex_property_frame, key, vertex_property_cols):
        """
        Parameters
        ----------
        vertex_property_frame: a frame holding the additional vertex properties to register
        key: the name of the vertex id column used to match the vertices that are already registered
        vertex_property_cols: list of columns in the specified vertex_property_frame to use for vertex properties 
        
        Examples
        ----------
        >>> gb = get_graph_builder(GraphTypes.Property, my_frame)
        >>> gb.register_vertex_properties(v_prop_frame, 'id', ['name', 'age', 'dept'])
        """
        self.registered_vertex_properties = GraphBuilderVertexProperties(vertex_property_frame, key, vertex_property_cols)         

    def register_edge(self, edge_tuple, properties=None):
        """
        Specify edge creation rule

        Parameters
        ----------
        edge : tuple
            Tuple of keys for source, target, and label.
        properties : list
            List of property source keys.

        Examples
        --------
        >>> gb = get_graph_builder(GraphTypes.Property, my_frame)
        >>> gb.register_edge(('id', 'manager', 'works_for'), ['rate', 'hours'])
        """
        self._edge_list.append(GraphBuilderEdge(edge_tuple, properties))

    def register_edges(self, edges):
        """
        Specify edge creation rules (see register_edge)

        Parameters
        ----------
        edges : list of tuples
            List of tuples of tuples and lists.

        Examples
        --------
        >>> gb = get_graph_builder(GraphTypes.Property, my_frame)
        >>> gb.register_edges([(('id', 'manager', 'works_for'), ['rate', 'hours']), (('id', 'bldg', 'location'), [])])
        """
        for edge in edges:
            if len(edge) != 2:
                raise ValueError("ERROR: Incorrect edge tuple: " + str(edge))
            self.register_edge(edge[0], edge[1])
            
    def register_edge_properties(self, edge_property_frame, edge_tuple, edge_property_cols):
        """
        Parameters
        ----------
        edge_property_frame: a frame holding the additional edge properties to register
        edge_tuple: the tuple specifying the edge id (src_vertex_id, dest_vertex_id, edge_label) used to match the edges that are already registered
        edge_property_cols: list of columns in the specified edge_property_frame to use for edge properties 
        
        Examples
        ----------
        >>> gb = get_graph_builder(GraphTypes.Property, my_frame)
        >>> gb.register_edge_properties(v_prop_frame, 'id', ['name', 'age', 'dept'])
        """
        self.registered_edge_properties = GraphBuilderEdgeProperties(edge_property_frame, edge_tuple, edge_property_cols)                     

#TODO would be nice to use GraphBuilderVertexProperties/GraphBuilderEdgeProperties
#for all properties, not only for properties added with register_vertex/edge_properties 
class GraphBuilderVertexProperties:
    """
    Class for holding GraphBuilder vertex properties that can be built from different frames
    """
    def __init__(self, source_frame, key, vertex_property_cols):
        self.source_frame = source_frame
        self.vertex = GraphBuilderVertex(key, vertex_property_cols)
    
    def __str__(self):
        return  "GraphBuilderVertexProperties: {0}, {1}".format(self.source_frame.__str__(), self.vertex.__str__())        
        
class GraphBuilderEdgeProperties:
    """
    Class for holding GraphBuilder edge properties that can be built from different frames
    """
    def __init__(self, source_frame, edge_tuple, edge_property_cols):
        self.source_frame = source_frame
        self.edge = GraphBuilderEdge(edge_tuple, edge_property_cols) 
        
    def __str__(self):
        return  "GraphBuilderEdgeProperties: {0}, {1}".format(self.source_frame.__str__(), self.edge.__str__())
    
class GraphBuilderVertex:
    """
    An entry for GraphBuilder vertex registration.
    """
    def __init__(self, key, properties=None):
        if key is None:
            raise Exception("Invalid key_src: None")
        if properties is None:
            properties = []
        self.key = key
        self.properties = properties

    def __repr__(self):
        return "'{0}', [{1}]".format(self.key,
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
        if self.source is None:
            raise Exception("Invalid edge source: None")
        if self.target is None:
            raise Exception("Invalid edge target: None")
        if self.label is None:
            raise Exception("Invalid edge label: None")
        if properties is None:
            properties = []
        self.properties = properties

    def __repr__(self):
        return "('{0}','{1}','{2}'), [{3}]".format(self.source,
                                            self.target,
                                            self.label,
                                            ("'" + "', '".join(self.properties)
                                             + "'")
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


def get_graph_builder(graph_type, source=None):
    """
    Returns a new graph builder

    Gets a graph builder which will build the given type of graph for the
    given source

    Parameters
    ----------
    graph_type : (GraphTypes.Property | GraphTypes.Bipartite)
        the type of graph to create.
    source : BigDataFrame
        the source of the data for the graph being built

    Returns
    -------
    graph_builder : GraphBuilder
        new graph builder object

    Examples
    --------
    >>> gb = get_graph_builder(GraphTypes.Property, my_frame)
    """
    factory_class = _get_graph_builder_factory_class()
    return factory_class.get_graph_builder(graph_type, source)


def get_graph(graph_name):
    """
    Returns a graph object for a previously created graph

    Parameters
    ----------
    graph_name : string
        user-given name of a previously created graph

    Returns
    -------
    graph : Graph
        new graph object
    """
    factory_class = _get_graph_builder_factory_class()
    return factory_class.get_graph(graph_name)


def get_graph_names():
    """
    Returns a list of names of all the available, previously created graphs

    Returns
    -------
    names : list
    """
    factory_class = _get_graph_builder_factory_class()
    return factory_class.get_graph_names()

# dynamically and lazily load the correct graph_builder factory,
# according to config
_graph_builder_factory = None


def _get_graph_builder_factory_class():
    global _graph_builder_factory
    if _graph_builder_factory is None:
        graph_builder_factory_class = dynamic_import(
            global_config['py_graph_builder_factory_class'])
        _graph_builder_factory = graph_builder_factory_class.get_instance()
    return _graph_builder_factory
