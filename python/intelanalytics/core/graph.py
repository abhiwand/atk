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
import uuid

from intelanalytics.core.serialize import to_json
from intelanalytics.core.column import BigColumn
from intelanalytics.core.metaprog import CommandLoadable

try:
    from intelanalytics.core.autograph import CommandLoadableBigGraph
    logger.info("BigGraph is inheriting commands from autograph.py")
except:
    msg = "autograph.py not found, BigGraph is NOT inheriting commands from it"
    logger.warn(msg)
    import warnings
    warnings.warn(msg, RuntimeWarning)
    CommandLoadableBigGraph = CommandLoadable

from intelanalytics.core.deprecate import deprecated


def _get_backend():
    from intelanalytics.core.config import get_graph_backend
    return get_graph_backend()


def get_graph_names():
    """
    Get graph names.

    Gets the names of BigGraph objects available for retrieval.

    Returns
    -------
    list of string
        A list comprised of the graph names

    Examples
    --------
    We have these graphs defined: movies, incomes, virus.
    Get the graph names::

        my_names = ia.get_graph_names()

    my_names is now ["incomes", "movies", "virus"]

    .. versionadded:: 0.8

    """
    # TODO - Review docstring
    return _get_backend().get_graph_names()


def get_graph(name):
    """
    Get graph access.

    Creates a BigGraph access point to the named graph.

    Parameters
    ----------
    name : string
        The name of the graph you are obtaining

    Returns
    -------
    graph
        A BigGraph object

    Examples
    --------
    We have these graphs defined: movies, incomes, virus.
    Get access to the graph *virus*::

        my_graph = ia.get_graph("virus")

    my_graph is now a BigGraph object with access to the graph *virus*.

    .. versionadded:: 0.8

    """
    # TODO - Review docstring
    return _get_backend().get_graph(name)


@deprecated("use drop_graphs")
def delete_graph(name):
    return drop_graphs(name)


def drop_graphs(graphs):
    """
    Deletes graphs from backing store.

    Parameters
    ----------
    graphs : string or BigGraph
        Either the name of the BigGraph object to delete or the BigGraph object itself

    Returns
    -------
    string
        The name of the graph you erased

    Examples
    --------
    We have these graphs defined: movies, incomes, virus.
    Delete the graph *incomes*::

        my_gone = ia.drop_graphs("incomes")

    my_gone is now a string with the value "incomes"

    .. versionchanged:: 0.8.5

    """
    # TODO - Review docstring
    return _get_backend().delete_graph(graphs)


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
        if isinstance(source, BigColumn):
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
    properties: dictionary (optional)
        vertex properties of the form property_name:property_value
        property_name is a string, and property_value is a literal value
        or a BigColumn source, which must be from same BigFrame as value arg

    Examples
    --------
    ::

        movie_vertex = ia.VertexRule('movie', my_frame['movie'], {'genre':
            my_frame['genre']})
        user_vertex = ia.VertexRule('user', my_frame['user'], {'age':
            my_frame['age_1']})

    .. versionadded:: 0.8

    """
    def __init__(self, id_key, id_value, properties=None):
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

            my_graph = BigGraph(my_rule_a, my_rule_b, my_rule_1)
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
        edge label, can be constant string or pulled from BigColumn.
    tail: VertexRule
        tail vertex ('from' vertex); must be from same BigFrame as head,
        label and any properties
    head: VertexRule
        head vertex ('to' vertex); must be from same BigFrame as tail,
        label and any properties
    properties: dict
        edge properties of the form property_name:property_value
        property_name is a string, and property_value is a literal value
        or a BigColumn source, which must be from same BigFrame as head,
        tail and label
    is_directed : bool
        indicates the edge is directed

    Examples
    --------
    ::

        rating_edge = ia.EdgeRule('rating', movie_vertex, user_vertex, {'weight':
            my_frame['score']})

    .. versionadded:: 0.8

    """
    def __init__(self, label, tail, head, properties=None, is_directed=False):
        self.label = label
        self.tail = tail
        self.head = head
        self.properties = properties or {}
        self.is_directed = bool(is_directed)
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
        if isinstance(self.label, BigColumn):
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


class BigGraph(CommandLoadableBigGraph):
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
    This example uses a single source data frame and creates a graph of 'user' and 'movie' vertices connected by
    'rating' edges::

        # create a frame as the source for a graph
        csv = ia.CsvFile("/movie.csv", schema= [('user', int32),
                                            ('vertexType', str),
                                            ('movie', int32),
                                            ('rating', str)])
        frame = ia.BigFrame(csv)

        # define graph parsing rules
        user = ia.VertexRule("user", frame.user, {"vertexType": frame.vertexType})
        movie = ia.VertexRule("movie", frame.movie)
        rates = ia.EdgeRule("rating", user, movie, { "rating": frame.rating },
            is_directed = True)

        # create graph
        graph = ia.BigGraph([user, movie, rates])

    .. versionadded:: 0.8

    """

    # command load filters:
    _command_prefixes = ['graph', 'graphs']
    _muted_command_names = ['rename_graph']  # these commands are not exposed

    def __init__(self, rules=None, name=""):
        try:
            self._id = 0
            self._ia_uri = None
            if not hasattr(self, '_backend'):
                self._backend = _get_backend()
            CommandLoadableBigGraph.__init__(self)
            new_graph_name= self._backend.create(self, rules, name)
            logger.info('Created new graph "%s"', new_graph_name)
        except:
            raise IaError(logger)

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(BigGraph,self).__repr__() + "(Unable to collect metadeta from server)"

    @property
    def name(self):
        """
        Get the name of the current object.

        Returns
        -------
        string
            The name of the current object.

        Examples
        --------
        ::

            my_graph = ia.BigGraph( , "my_data")
            my_name = my_graph.name

        my_name is now a string with the value "my_data"

        .. versionadded:: 0.8

        """
        # TODO - Review Docstring
        try:
            return self._backend.get_name(self)
        except:
            IaError(logger)

    @name.setter
    def name(self, value):
        """
        Set the name of the current object.

        Parameters
        ----------
        value : string
            The name for the current object.

        Examples
        --------
        ::

            my_graph = ia.BigGraph()
            my_graph.name("my_data")

        my_graph is now a BigGraph object with the name "my_data"

        .. versionadded:: 0.8

        """
        # TODO - Review Docstring
        try:
            self._backend.rename_graph(self, value)
        except:
            raise IaError(logger)

    @property
    def ia_uri(self):
        try:
            return self._backend.get_ia_uri(self)
        except:
            raise IaError(logger)

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

            frame = ia.BigFrame(csv)

            # define graph parsing rules
            user = ia.VertexRule("user", frame.user, {"vertexType": frame.vertexType})
            movie = ia.VertexRule("movie", frame.movie)
            rates = ia.EdgeRule("rating", user, movie, { "rating": frame.rating },
                is_directed = True)

            # append data from the frame to an existing graph
            graph.append([user, movie, rates])

        This example shows creating a graph from one frame and appending data to it from other frames::

            # create a frame as the source for a graph
            ratingsFrame = ia.BigFrame(ia.CsvFile("/ratings.csv", schema= [('userId', int32),
                                                  ('movieId', int32),
                                                  ('rating', str)]))

            # define graph parsing rules
            user = ia.VertexRule("user", ratingsFrame.userId)
            movie = ia.VertexRule("movie", ratingsFrame.movieId)
            rates = ia.EdgeRule("rating", user, movie, { "rating": ratingsFrame.rating },
                is_directed = True)

            # create graph
            graph = ia.BigGraph([user, movie, rates])

            # load additional properties onto the user vertices
            usersFrame = ia.BigFrame(ia.CsvFile("/users.csv", schema= [('userId', int32),
                ('name', str), ('age', int32)]))
            userAdditional = ia.VertexRule("user", usersFrame.userId, {"userName":
                usersFrame.name, "age": usersFrame.age })
            graph.append([userAdditional])

            # load additional properties onto the movie vertices
            movieFrame = ia.BigFrame(ia.CsvFile("/movies.csv", schema= [('movieId', int32),
                ('title', str), ('year', int32)]))
            movieAdditional = ia.VertexRule("movie", movieFrame.movieId, {"title":
                movieFrame.title, "year": movieFrame.year })
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

