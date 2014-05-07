
# New Graph Builder API proposal

# Here's a full example that should make more sense after reading the code below
# (where f and f2 are data frames)
#
#
# >>> movie_vertex = VertexRule('movie', f['movie'], genre=f['genre'])
#
# >>> user_vertex = VertexRule('user', f['user'], age=f['age_1'])
#
# >>> rating_edge = EdgeRule('rating', movie_vertex, user_vertex, weight=f['score'])
#
# >>> extra_movie_rule = VertexPropertyRule(vertex_movie, f2['movie'], oscars=f2['oscars'])
#
# >>> g = BigGraph([user_vertex, movie_vertex, rating_edge, extra_movie_rule])
#
# >>> extra_rating_rule = EdgePropertyRule(rating_edge, f2['movie'], f2['user'], rotten_tomatoes=f2['rt'])
#
# >>> g.add_props([extra_rating_rule])
#



class Rule(object):
    """
    Graph building rule base class

    ============= =========== =========================================================== ==============
    Parameter     Data Type   Meaning                                                     Default Value
    ============= =========== =========================================================== ==============
    object        ?                                                                       Not optional
    ============= =========== =========================================================== ==============


    **Examples**

    >>> 
    """
    pass


class VertexRule(Rule):
    """
    Specifies creation of a vertex

    ============= =========== =========================================================== ==============
    Parameter     Data Type   Meaning                                                     Default Value
    ============= =========== =========================================================== ==============
    label         str or      vertex label, static string or pulled from BigColumn source Not optional
                  BigColumn
    value         BigColumn   vertex value                                                Not optional
    **props       Dictionary  vertex properties of the form property_name:property_value  Optional
                              property_name is a string, and property_value is a literal
                              value or a BigColumn source, which must be from same
                              BigFrame as value arg
    ============= =========== =========================================================== ==============


    **Examples**

    >>> movie_vertex = VertexRule('movie', f['movie'], genre=f['genre'])
    >>> user_vertex = VertexRule('user', f['user'], age=f['age_1'])
    """
    def __init__(self, label, value, **props):
        pass


class EdgeRule(Rule):
    """
    Specifies creation of an edge

    ============= =========== =========================================================== ==============
    Parameter     Data Type   Meaning                                                     Default Value
    ============= =========== =========================================================== ==============
    label         str or      vertex label, static string or pulled from BigColumn source Not optional
                  BigColumn

    src           VertexRule  source vertex; must be from same BigFrame as dst            Not optional
    dst           VertexRule  source vertex; must be from same BigFrame as src            Not optional
    is_directed   bool        indicates the edge is directed                              False
    **props       Dictionary  vertex properties of the form property_name:property_value  Optional
                              property_name is a string, and property_value is a literal
                              value or a BigColumn source, which must be from same
                              BigFrame as value src, dst
    ============= =========== =========================================================== ==============


    **Examples**

    >>> rating_edge = EdgeRule('rating', movie_vertex, user_vertex, weight=f['score'])
    """
    def __init__(self, label, src, dst, is_directed=False, **props):
        pass


class PropertyRule(Rule):
    """
    To do

    ============= =========== =========================================================== ==============
    Parameter     Data Type   Meaning                                                     Default Value
    ============= =========== =========================================================== ==============
    \ 
    ============= =========== =========================================================== ==============


    **Examples**

    >>> 
    """
    pass

class VertexPropertyRule(PropertyRule):
    """
    Specifies attachment of additional properties to a vertex

    ============= =========== =========================================================== ==============
    Parameter     Data Type   Meaning                                                     Default Value
    ============= =========== =========================================================== ==============
    vertex        VertexRule  target vertex for property attachment                       Not optional
    match_value   BigColumn   # more appropriate name?
                              BigColumn source whose value must match against the value
                              of the target vertex
    **props       dict        vertex properties of the form property_name:property_value
                              property_name is a string, and property_value is a literal
                              value or a BigColumn source
    ============= =========== =========================================================== ==============


    **Examples**
        
    >>> extra_movie_rule = VertexPropertyRule(vertex_movie, f2['movie'], oscars=f2['oscars'])
    """
    def __init__(self, vertex, match_value, **props):
        pass


class EdgePropertyRule(PropertyRule):
    """
    Specifies attachment of additional properties to an edge

    ============= =========== =========================================================== ==============
    Parameter     Data Type   Meaning                                                     Default Value
    ============= =========== =========================================================== ==============
    edge          EdgeRule    target edge for property attachment                         Not optional
    match_src     BigColumn   # more appropriate name?                                    Not optional
                              BigColumn source whose value must match against the value
                              of the target edge's source vertex
    match_dst     BigColumn   # more appropriate name?                                    Not optional
                              BigColumn source whose value must match against the value
                              of the target edge's destination vertex
    **props       dict        edge properties of the form property_name:property_value    Optional
                              property_name is a string, and property_value is a literal
                              value or a BigColumn source
    ============= =========== =========================================================== ==============

    **Examples**

    >>> extra_rating_rule = EdgePropertyRule(rating_edge, f2['movie'], f2['user'], rotten_tomatoes=f2['rt'])
    """
    def __init__(self, edge, match_src, match_dst, **props):
        pass


class GraphRule(Rule):
    """
    Specifies graph properties

    ============= =========== =========================================================== ==============
    Parameter     Data Type   Meaning                                                     Default Value
    ============= =========== =========================================================== ==============
    vid           str         the name of the vertex id  # ie today's _gb_ID              None
    ============= =========== =========================================================== ==============

    **Examples**

    >>> extra_rating_rule = EdgePropertyRule(rating_edge, f2['movie'], f2['user'], rotten_tomatoes=f2['rt'])
    """
    def __init__(self, vid=None):
        pass


class BigGraph(object):
    """
    Creates a big graph

    ============= =========== =========================================================== ==============
    Parameter     Data Type   Meaning                                                     Default Value
    ============= =========== =========================================================== ==============
    rules         list of     list of rules which specify how the graph will be created   None
                  rules
    ============= =========== =========================================================== ==============

    **Examples**
    >>> g = BigGraph([user_vertex, movie_vertex, rating_edge, extra_movie_rule])
    """
    def __init__(self, rules=None):
        pass

    def add_props(self, rules):
        """
        Adds properties to existing vertices and edges

        ============= ============ =========================================================== ==============
        Parameter     Data Type    Meaning                                                     Default Value
        ============= ============ =========================================================== ==============
        rules         list of      list of property rules which specify how to add the         Not optional
                      PropertyRule properties
        ============= ============ =========================================================== ==============

        **Examples**

        >>> g.add_props([extra_rating_rule])
        """
        pass


