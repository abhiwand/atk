##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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
from intelanalytics.core.errorhandle import IaError

f, f2 = {}, {}

import logging
logger = logging.getLogger(__name__)
from intelanalytics.meta.api import get_api_decorator, check_api_is_loaded
api = get_api_decorator(logger)

from intelanalytics.meta.metaprog import CommandLoadable, doc_stubs_import
from intelanalytics.meta.namedobj import name_support
import uuid

from intelanalytics.meta.serialize import to_json
from intelanalytics.core.column import Column

from intelanalytics.core.deprecate import raise_deprecation_warning

titan_rule_deprecation = """
EdgeRule and VertexRule graph construction objects are deprecated.
Instead, construct a Graph object, then define and add vertices and
edges directly.  export_to_titan is available to obtain a TitanGraph.

Example:

>>> import intelanalytics as ia
>>> g = ia.Graph()
>>> g.define_vertex_type('users')
>>> g.define_vertex_type('machines')
>>> g.vertices['users'].add_vertices(source_frame1, 'user')
>>> g.vertices['machines'].add_vertices(source_frame2, 'machine')
>>> g.define_edge_type('links', 'users', 'machines', directed=False)

>>> t = g.export_to_titan()
"""

__all__ = ["drop_frames", "drop_graphs", "EdgeRule", "Frame", "get_frame", "get_frame_names", "get_graph", "get_graph_names", "TitanGraph", "VertexRule"]

def _get_backend():
    from intelanalytics.meta.config import get_graph_backend
    return get_graph_backend()


class RuleWithDifferentFramesError(ValueError):
    # TODO - Add docstring if this is really a user-desired function
    def __init__(self):
        ValueError.__init__(self, "Rule contains columns from different frames")


# TODO - make an Abstract Class
class Rule(object):
    """
    Graph rule base class.

    .. versionadded:: 0.8

    """
    # TODO - Docstrings

    def __init__(self):
        self.source_frame = self._validate()

    # A bunch of rule validation methods, each of which returns the common
    # source frame for the rule.  A little extra validation work here to enable
    # an easier API for the interactive user

    # Must be overridden:
    def _validate(self):
        """

        .. versionadded:: 0.8

        """
        # TODO - Docstrings
        raise NotImplementedError

    @staticmethod
    def _validate_source(source, frame):
        """
        Source: String or BigColumn.

        .. versionadded:: 0.8

        """
        # TODO - Add examples
        if isinstance(source, Column):
            if frame is None:
                frame = source.frame
            elif frame != source.frame:
                raise RuleWithDifferentFramesError()
        elif not isinstance(source, basestring):
                raise TypeError("Rule contains invalid source type: " + type(source).__name__)
        return frame

    @staticmethod
    def _validate_property(key, value, frame):
        """

        .. versionadded:: 0.8

        """
        # TODO - Docstrings
        frame = Rule._validate_source(key, frame)
        frame = Rule._validate_source(value, frame)
        return frame

    @staticmethod
    def _validate_properties(properties):
        """

        .. versionadded:: 0.8

        """
        # TODO - Docstrings
        frame = None
        if properties:
            for k, v in properties.items():
                frame = Rule._validate_property(k, v, frame)
        return frame

    @staticmethod
    def _validate_same_frame(*frames):
        """
        Assures all non-None frames provided are in fact the same frame.

        .. versionadded:: 0.8

        """
        # TODO - Docstrings
        same = None
        for f in frames:
            if f:
                if same and f != same:
                    raise RuleWithDifferentFramesError()
                else:
                    same = f
        return same


class VertexRule(Rule):
    """
    Specifies a vertex and vertex properties.

    Dynamically pulling property names from a BigColumn can have a negative
    performance impact if there are many distinct values (hundreds of
    values are okay, thousands of values may take a long time).

    Parameters
    ----------
    id_key: string
        static string or pulled from BigColumn source; the key for the
        uniquely identifying property for the vertex.
    id_value: BigColumn source
        vertex value; the unique value to identify this vertex
    properties: dictionary {'vertex_type': ['L|R'] [, property_name:property_value]}
        vertex properties of the form property_name:property_value.
        The property_name (the key) is a string, and property_value is a literal value
        or a BigColumn source, which must be from the same Frame as the id_key and id_value arguments.

    Notes
    -----
    Vertex rules must include the property 'vertex_type':'L' for left-side, or 'vertex_type':'R' for
    right-side, for the ALS and CGD (and other) algorithms to work properly.

    Examples
    --------
    ::

        movie_vertex = ia.VertexRule('movie', my_frame['movie'], {'genre': my_frame['genre'], 'vertex_type':'L'})
        user_vertex = ia.VertexRule('user', my_frame['user'], {'age': my_frame['age_1'], 'vertex_type':'R'})

    .. versionadded:: 0.8

    """
    def __init__(self, id_key, id_value, properties=None):
        raise_deprecation_warning("VertexRule", titan_rule_deprecation)
        self.id_key = id_key
        self.id_value = id_value
        self.properties = properties or {}
        super(VertexRule, self).__init__()  # invokes validation

    def _as_json_obj(self):
        """JSON from point of view of Python API, NOT the REST API"""
        d = dict(self.__dict__)
        del d['source_frame']
        return d

    def __repr__(self):
        return to_json(self)

    def _validate(self):
        """
        Checks that the rule has what it needs.

        Returns
        -------

        Examples
        --------
        ::

            my_graph = Graph(my_rule_a, my_rule_b, my_rule_1)
            validation = my_graph.validate()

        .. versionadded:: 0.8

        """

        # TODO - Add docstring
        id_frame = self._validate_property(self.id_key, self.id_value, None)
        properties_frame = self._validate_properties(self.properties)
        return self._validate_same_frame(id_frame, properties_frame)


class EdgeRule(Rule):
    """
    Specifies an edge and edge properties.

    Dynamically pulling labels or property names from a BigColumn can
    have a negative performance impact if there are many distinct values
    (hundreds of values are okay, thousands of values may take a long time).

    Parameters
    ----------
    label: str or BigColumn source
        Edge label, can be constant string or pulled from BigColumn.
    tail: VertexRule
        Tail vertex ('from' vertex); must be from same Frame as head,
        label and any properties.
    head: VertexRule
        Head vertex ('to' vertex); must be from same Frame as tail,
        label and any properties.
    properties: dict
        Edge properties of the form property_name:property_value
        property_name is a string, and property_value is a literal value
        or a BigColumn source, which must be from same Frame as head,
        tail and label.
    bidirectional : bool (optional)
        Indicates the edge is bidirectional. The default value is false.

    Examples
    --------
    ::

        rating_edge = ia.EdgeRule('rating', movie_vertex, user_vertex, {'weight': my_frame['score']})

    .. versionadded:: 0.8

    """
    def __init__(self, label, tail, head, properties=None, bidirectional=False, is_directed=None):
        raise_deprecation_warning("EdgeRule", titan_rule_deprecation)
        self.bidirectional = bool(bidirectional)
        if is_directed is not None:
            raise_deprecation_warning("EdgeRule", "Parameter 'is_directed' is now called bidirectional' and has opposite polarity.")
            self.bidirectional = not is_directed

        self.label = label
        self.tail = tail
        self.head = head
        self.properties = properties or {}

        super(EdgeRule, self).__init__()  # invokes validation


    def _as_json_obj(self):
        """JSON from point of view of Python API, NOT the REST API"""
        d = dict(self.__dict__)
        d['tail'] = None if not self.tail else self.tail._as_json_obj()
        d['head'] = None if not self.head else self.head._as_json_obj()
        del d['source_frame']
        return d

    def __repr__(self):
        return to_json(self)

    def _validate(self):
        """
        Checks that the rule has what it needs.

        Raises
        ------
        TypeError
            "Label argument must be a column or non-empty string"

        Returns
        -------

        Examples
        --------
        ::

            Example

        .. versionadded:: 0.8

        """
        # TODO - Add docstring

        label_frame = None
        if isinstance(self.label, Column):
            label_frame = VertexRule('label', self.label)._validate()
        elif not self.label or not isinstance(self.label, basestring):
            raise TypeError("label argument must be a column or non-empty string")

        if isinstance(self.tail, VertexRule):
            tail_frame = self.tail._validate()
        else:
            raise TypeError("Invalid type %s for 'tail' argument. It must be a VertexRule." % self.tail)

        if isinstance(self.head, VertexRule):
            head_frame = self.head._validate()
        else:
            raise TypeError("Invalid type %s for 'head' argument. It must be a VertexRule." % self.head)
        properties_frame = self._validate_properties(self.properties)
        return self._validate_same_frame(label_frame, tail_frame, head_frame, properties_frame)


# _BaseGraph
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubsBaseGraph
    doc_stubs_import.success(logger, "DocStubsBaseGraph")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsBaseGraph", e)
    class DocStubsBaseGraph(object): pass


# TitanGraph
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubsTitanGraph
    doc_stubs_import.success(logger, "DocStubsTitanGraph")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsTitanGraph", e)
    class DocStubsTitanGraph(object): pass


# Graph
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubsGraph
    doc_stubs_import.success(logger, "DocStubsGraph")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsGraph", e)
    class DocStubsGraph(object): pass


@api
@name_support('graph')
class _BaseGraph(DocStubsBaseGraph, CommandLoadable):
    _command_prefix = 'graph'
    def __init__(self):
        CommandLoadable.__init__(self)

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(_BaseGraph, self).__repr__() + " (Unable to collect metadata from server)"


@api
class Graph(DocStubsGraph, _BaseGraph):
    """
    Creates a property Graph.

    This Graph is a collection of Vertex and Edge lists stored as frames.  This allows frame-like
    operations against graph data.  Many frame methods are available against vertices and edges.
    Vertex and Edge properties are stored as columns.

    Graph is better suited for bulk OLAP-type operations whereas TitanGraph is better suited to OLTP.

    Examples
    --------
    This example uses a single source data frame and creates a graph of 'user' and 'movie' vertices
    connected by 'rating' edges::

        # create a frame as the source for a graph
        csv = ia.CsvFile("/movie.csv", schema= [('user_id', int32),
                                            ('user_name', str),
                                            ('movie_id', int32),
                                            ('movie_title', str),
                                            ('rating', str)])
        frame = ia.Frame(csv)

        # create a graph
        graph = ia.Graph()

        # define the types of vertices and edges this graph will be made of
        graph.define_vertex_type('users')
        graph.define_vertex_type('movies')
        graph.define_edge_type('ratings','users','movies',directed=True)

        # add data to the graph
        graph.vertices['users'].add_vertices(frame, 'user_id', ['user_name'])
        graph.vertices['movies].add_vertices(frame, 'movie_id', ['movie_title])
        graph.edges['ratings'].add_edges(frame, 'user_id', 'movie_id', ['rating']

        # append additional data to the graph from another frame
        graph.vertices['users'].add_vertices(frame2, 'user_id', ['user_name'])

        # get basic information about the graph
        graph.vertex_count()
        graph.edge_count()
        graph.vertices['users'].inspect(20)


    This example uses a multiple source data frames and creates a graph of 'user' and 'movie' vertices
    connected by 'rating' edges::

        # create a frame as the source for a graph
        userFrame = ia.Frame(ia.CsvFile("/users.csv", schema= [('user_id', int32),
                                            ('user_name', str),
                                            ('age', int32)]))

        movieFrame = ia.Frame(ia.CsvFile("/movie.csv", schema= [('movie_id', int32),
                                            ('movie_title', str),
                                            ('year', str)]))

        ratingsFrame = ia.Frame(ia.CsvFile("/ratings.csv", schema= [('user_id', int32),
                                            ('movie_id', int32),
                                            ('rating', str)]))

        # create a graph
        graph = ia.Graph()

        # define the types of vertices and edges this graph will be made of
        graph.define_vertex_type('users')
        graph.define_vertex_type('movies')
        graph.define_edge_type('ratings','users','movies',directed=True)

        # add data to the graph
        graph.vertices['users'].add_vertices(userFrame, 'user_id', ['user_name', 'age'])
        graph.vertices['movies].add_vertices(movieFrame, 'movie_id') # all columns automatically added as properties
        graph.edges['ratings'].add_edges(frame, 'user_id', 'movie_id', ['rating']

    This example shows edges between vertices of the same type.  In this example, "employees work under other employees" ::

        # create a frame to use as the source for the graph data
        employees_frame = ia.Frame(ia.CsvFile("employees.csv", schema = [('Employee', str), ('Manager', str), ('Title', str), ('Years', ia.int64)], skip_header_lines=1), 'employees_frame')

        # define a graph
        graph = ia.Graph()
        graph.define_vertex_type('Employee')
        graph.define_edge_type('worksunder', 'Employee', 'Employee', directed=True)

        # add data
        graph.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
        graph.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'], create_missing_vertices = True)

        # inspect the graph
        graph.vertex_count()
        graph.edge_count()
        graph.vertices['Employee'].inspect(20)
        graph.edges['worksunder'].inspect(20)

    """
    _command_prefix = 'graph:'

    def __init__(self, source=None, name=''):
        if not hasattr(self, '_backend'):
            self._backend = _get_backend()
        from intelanalytics.rest.graph import GraphInfo
        if isinstance(source, dict):
            source = GraphInfo(source)
        if isinstance(source, GraphInfo):
            self._id = source.id_number
        elif source is None:
            self._id = self._backend.create(self, None, name, 'ia/frame')
        else:
            raise ValueError("Invalid source value of type %s" % type(source))

        self.vertices = GraphFrameCollection(self._get_vertex_frame, self._get_vertex_frames)
        self.edges = GraphFrameCollection(self._get_edge_frame, self._get_edge_frames)

        _BaseGraph.__init__(self)

    @api
    def _get_vertex_frame(self, label):
        """
        return a VertexFrame for the associated label
        :param label: the label of the frame to return
        """
        return self._backend.get_vertex_frame(self._id, label)

    @api
    def _get_vertex_frames(self):
        """
        return all VertexFrames for this graph
        """
        return self._backend.get_vertex_frames(self._id)

    @api
    def _get_edge_frame(self, label):
        """
        return an EdgeFrame for the associated label
        :param label: the label of the frame to return
        """
        return self._backend.get_edge_frame(self._id, label)

    @api
    def _get_edge_frames(self):
        """
        return all EdgeFrames for this graph
        """
        return self._backend.get_edge_frames(self._id)

    @property
    @api
    def vertex_count(self):
        """
        Get the total number of vertices in the graph.

        Examples
        --------
        graph.vertex_count

        The result given is::

            1194

        .. versionadded:: 0.9

        """
        return self._backend.get_vertex_count(self)

    @property
    @api
    def edge_count(self):
        """
        Get the total number of edges in the graph.

        Examples
        --------
        graph.edge_count

        The result given is::

            1194

        .. versionadded:: 0.9

        """
        return self._backend.get_edge_count(self)


class GraphFrameCollection(object):
    """
    This class represents a collection of frames that make up either the edge or vertex types of a graph
    """

    def __init__(self, get_frame_func, get_frames_func):
        """
        :param get_frame_func: method to call to return a single frame in the collection
        :param get_frames_func: method to call to return all of the frames in the collection
        """
        self.get_frame_func = get_frame_func
        self.get_frames_func = get_frames_func

    def __getitem__(self, item):
        """
        Retrieve a single frame from the collection
        :param item:
        """
        return self.get_frame_func(item)

    def __iter__(self):
        """
        iterator for all of the frames in the collection. will call the server
        """
        for frame in self.get_frames_func():
            yield frame

    def __repr__(self):
        """
        printable representation of object
        """
        return repr(self.get_frames_func())


@api
class TitanGraph(DocStubsTitanGraph, _BaseGraph):
    """
    Creates a big graph.

    Parameters
    ----------
    rules : list of Rule (optional)
         list of rules which specify how the graph will be created; if empty
         an empty graph will be created
    name : str (optional)
         name for the new graph; if not provided a default name is generated

    Examples
    --------
    This example uses a single source data frame and creates a graph of 'user' and 'movie' vertices
    connected by 'rating' edges::

        # create a frame as the source for a graph
        csv = ia.CsvFile("/movie.csv", schema= [('user', int32),
                                            ('vertexType', str),
                                            ('movie', int32),
                                            ('rating', str)])
        frame = ia.BigFrame(csv)

        # define graph parsing rules
        user = ia.VertexRule("user", frame.user, {"vertexType": frame.vertexType})
        movie = ia.VertexRule("movie", frame.movie)
        rates = ia.EdgeRule("rating", user, movie, { "rating": frame.rating }, bidirectional = True)

        # create graph
        graph = ia.TitanGraph([user, movie, rates])

    .. versionadded:: 0.8

    """

    _command_prefix = 'graph:titan'

    def __init__(self, rules=None, name=""):
        try:
            check_api_is_loaded()
            self._id = 0
            self._ia_uri = None
            if not hasattr(self, '_backend'):
                self._backend = _get_backend()
            _BaseGraph.__init__(self)
            self._id = self._backend.create(self, rules, name, 'hbase/titan')
            # logger.info('Created new graph "%s"', new_graph_name)
        except:
            raise IaError(logger)

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(TitanGraph,self).__repr__() + "(Unable to collect metadeta from server)"

    @api
    def append(self, rules=None):
        """
        Append frame data to the current graph.
        Append updates existing edges and vertices or creates new ones/ if they do not exist.
        Vertices are considered the same if their id_key's and id_value's match.
        Edges are considered the same if they have the same source Vertex, destination Vertex, and label.

        Parameters
        ----------
        rules : list of Rule
            list of rules which specify how the graph will be added to; if empty
            no data will be added.

        examples
        --------
        This example shows appending new user and movie data to an existing graph::

            # create a frame as the source for additional data
            csv = ia.CsvFile("/movie.csv", schema= [('user', int32),
                                                ('vertexType', str),
                                                ('movie', int32),
                                                ('rating', str)])

            frame = ia.Frame(csv)

            # define graph parsing rules
            user = ia.VertexRule("user", frame.user, {"vertexType": frame.vertexType})
            movie = ia.VertexRule("movie", frame.movie)
            rates = ia.EdgeRule("rating", user, movie, { "rating": frame.rating }, bidirectional = True)

            # append data from the frame to an existing graph
            graph.append([user, movie, rates])

        This example shows creating a graph from one frame and appending data to it from other frames::

            # create a frame as the source for a graph
            ratingsFrame = ia.Frame(ia.CsvFile("/ratings.csv", schema= [('userId', int32),
                                                  ('movieId', int32),
                                                  ('rating', str)]))

            # define graph parsing rules
            user = ia.VertexRule("user", ratingsFrame.userId)
            movie = ia.VertexRule("movie", ratingsFrame.movieId)
            rates = ia.EdgeRule("rating", user, movie, { "rating": ratingsFrame.rating }, bidirectional = True)

            # create graph
            graph = ia.Graph([user, movie, rates])

            # load additional properties onto the user vertices
            usersFrame = ia.Frame(ia.CsvFile("/users.csv", schema= [('userId', int32), ('name', str), ('age', int32)]))
            userAdditional = ia.VertexRule("user", usersFrame.userId, {"userName": usersFrame.name, "age": usersFrame.age })
            graph.append([userAdditional])

            # load additional properties onto the movie vertices
            movieFrame = ia.Frame(ia.CsvFile("/movies.csv", schema= [('movieId', int32), ('title', str), ('year', int32)]))
            movieAdditional = ia.VertexRule("movie", movieFrame.movieId, {"title": movieFrame.title, "year": movieFrame.year })
            graph.append([movieAdditional])

        .. versionadded:: 0.8

        """
        self._backend.append(self, rules)

    def _get_new_graph_name(self):
        return "graph_" + uuid.uuid4().hex

    # TODO - consider:
    #def add(self, rules)
    #def remove(self, rules)
    #def add_props(self, rules)
    #def remove_props(self, rules)
