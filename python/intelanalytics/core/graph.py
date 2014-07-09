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
f, f2 = {}, {}

import logging
logger = logging.getLogger(__name__)
import uuid

from intelanalytics.core.serialize import to_json
from intelanalytics.core.column import BigColumn
from intelanalytics.core.command import CommandSupport, doc_stub

def _get_backend():
    from intelanalytics.core.config import get_graph_backend
    return get_graph_backend()


def get_graph_names():
    """
    Gets the names of BigGraph objects available for retrieval
    
    Returns
    -------
    list of string
        A list comprised of the graph names
        
    Examples
    --------
    >>>
    If we have these BigGraph objects defined: movies, incomes, virus
    >>> my_names = get_graph_names()
    my_names is now ["incomes", "movies", "virus"]
    
    """
    # TODO - Review docstring
    return _get_backend().get_graph_names()


def get_graph(name):
    """
    Retrieves the named BigGraph object
    
    Parameters
    ----------
    name : string
        The name of the BigGraph object you are obtaining
        
    Returns
    -------
    graph
        A BigGraph object
    
    Examples
    --------
    >>>
    If we have these BigGraph objects defined: movies, incomes, virus
    >>> my_graph = get_graph("virus")
    my_graph is now a BigGraph object with the name "virus"
        
    """
    # TODO - Review docstring
    return _get_backend().get_graph(name)


def delete_graph(name):
    """
    Deletes the graph from backing store
    
    Parameters
    ----------
    name : string
        The name of the BigGraph object you are erasing
        
    Returns
    -------
    string
        The name of the BigGraph object you erased
    
    Examples
    --------
    >>>
    If we have these BigGraph objects defined: movies, incomes, virus
    >>> my_gone = delete_graph("incomes")
    my_gone is now a string with the value "incomes"
    
    """
    # TODO - Review docstring
    return _get_backend().delete_graph(name)


class RuleWithDifferentFramesError(ValueError):
    # TODO - Add docstring if this is really a user-desired function
    def __init__(self):
        ValueError.__init__(self, "Rule contains columns from different frames")


# TODO - make an Abstract Class
class Rule(object):
    """Graph rule base class"""
    # TODO - Add docstring
    def __init__(self):
        self.source_frame = self.validate()

    # A bunch of rule validation methods, each of which returns the common
    # source frame for the rule.  A little extra validation work here to enable
    # an easier API for the interactive user

    # Must be overridden:
    def validate(self):
        raise NotImplementedError

    @staticmethod
    def validate_source(source, frame):
        """
        Checks that source is a BigColumn or a string. If it is neither, it raises an error.
        If the frame is None, it is assigned the frame from the source.
        If the frame is named and it differs from the source.frame, it raises an error.
        
        Parameters
        ----------
        source
            D
        frame : string
            D

        Raises
        ------
        RuleWithDifferentFramesError()
        TypeError
        
        Returns
        -------
        string
            The name of the frame
            
        Examples
        --------
        >>>
        
        """
        # TODO - Add examples
        if isinstance(source, BigColumn):
            if frame is None:
                frame = source.frame
            elif frame != source.frame:
                raise RuleWithDifferentFramesError()
        elif not isinstance(source, basestring):
                raise TypeError("Rule contains invalid source type" + type(source).__name__)
        return frame

    @staticmethod
    def validate_property(key, value, frame):
        # TODO - Add docstring
        frame = Rule.validate_source(key, frame)
        frame = Rule.validate_source(value, frame)
        return frame

    @staticmethod
    def validate_properties(properties):
        # TODO - Add docstring
        frame = None
        if properties:
            for k, v in properties.items():
                frame = Rule.validate_property(k, v, frame)
        return frame

    @staticmethod
    def validate_same_frame(*frames):
        """Assures all non-None frames provided are in fact the same frame"""
        # TODO - Add docstring
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
    Specifies a vertex and vertex properties

    Parameters
    ----------
    id_key: string
        static string or pulled from BigColumn source; the key for the
        uniquely identifying property for the vertex.
    id_value: BigColumn source
        vertex value; the unique value to identify this vertex
    properties: dictionary
        vertex properties of the form property_name:property_value
        property_name is a string, and property_value is a literal value
        or a BigColumn source, which must be from same BigFrame as value arg

    Examples
    --------
    >>> movie_vertex = VertexRule('movie', f['movie'], {'genre': f['genre']})
    >>> user_vertex = VertexRule('user', f['user'], {'age': f['age_1']})
    
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

    def validate(self):
        """
        Checks that the rule has what it needs

        Returns
        -------

        Examples
        --------
        >>>

        """

        # TODO - Add docstring
        id_frame = self.validate_property(self.id_key, self.id_value, None)
        properties_frame = self.validate_properties(self.properties)
        return self.validate_same_frame(id_frame, properties_frame)


class EdgeRule(Rule):
    """
    Specifies an edge and edge properties

    Parameters
    ----------
    label: str or BigColumn source
        edge label, can be constant string or pulled from BigColumn
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
    >>> rating_edge = EdgeRule('rating', movie_vertex, user_vertex, {'weight': f['score']})
    
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

    def validate(self):
        """
        Checks that the rule has what it needs

        Raises
        ------
        TypeError
            "Label argument must be a column or non-empty string"

        Returns
        -------

        Examples
        --------
        >>>

        """
        # TODO - Add docstring
        label_frame = None
        if isinstance(self.label, BigColumn):
            label_frame = VertexRule('label', self.label).validate()
        elif not self.label or not isinstance(self.label, basestring):
            raise TypeError("label argument must be a column or non-empty string")
        tail_frame = self.tail.validate()
        head_frame = self.head.validate()
        properties_frame = self.validate_properties(self.properties)
        return self.validate_same_frame(label_frame, tail_frame, head_frame, properties_frame)


class BigGraph(CommandSupport):
    """
    Creates a big graph

    Parameters
    ----------
    rules : list of Rule
         list of rules which specify how the graph will be created; if empty
         an empty graph will be created
    name : str
         name for the new graph; if not provided a default name is generated

    Examples
    --------
    >>> g = BigGraph([user_vertex, movie_vertex, rating_edge, extra_movie_rule])

    """
    def __init__(self, rules=None, name=""):

        if rules and (not isinstance(rules, list) or not all([isinstance(rule, Rule) for rule in rules])):
            raise TypeError("rules must be a list of Rule objects")
        if not hasattr(self, '_backend'):
            self._backend = _get_backend()
        self._name = name or self._get_new_graph_name()
        self._uri = ""
        self._backend.create(self, rules)

        CommandSupport.__init__(self)
        #self.ml = GraphMachineLearning(self)
        logger.info('Created new graph "%s"', self._name)

    @property
    def name(self):
        """
        Get the name of the current ojbect.
        
        Returns
        -------
        string
            The name of the current object.
            
        Examples
        --------
        >>> my_graph = BigGraph( , "my_data")
        >>> my_name = my_graph.name
        my_name is now a string with the value "my_data"
        
        """
        # TODO - Review Docstring
        return self._name

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
        >>> my_graph = BigGraph()
        >>> my_graph.name("my_data")
        my_graph is now a BigGraph object with the name "my_data"
        
        """
        # TODO - Review Docstring
        self._backend.set_name(value)

    @property
    def uri(self):
        """
        Provides the URI of the BigGraph

        Returns
        -------
        URI
            See http://en.wikipedia.org/wiki/Uniform_Resource_Identifier
        """
        return self._uri

    def _get_new_graph_name(self):
        return "graph_" + uuid.uuid4().hex

    # TODO - consider:
    #def add(self, rules)
    #def remove(self, rules)
    #def add_props(self, rules)
    #def remove_props(self, rules)


class GraphMachineLearning(object):
    # TODO - Docstrings
    def __init__(self, graph):
        self.graph = graph
        if not hasattr(self, '_backend'):
            self._backend = _get_backend()

    def als(self,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type):
        self._backend.als(self.graph,
                          input_edge_property_list,
                          input_edge_label,
                          output_vertex_property_list,
                          vertex_type,
                          edge_type)

    def recommend(self, vertex_id):
        return self._backend.recommend(self.graph, vertex_id)
