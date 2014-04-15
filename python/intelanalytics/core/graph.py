
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
    """Graph building rule base class"""
    pass


class VertexRule(Rule):
    def __init__(self, label, value, **props):
        """
        Specifies creation of a vertex

        Parameters
        ----------
        label: str or BigColumn
            vertex label, static string or pulled from BigColumn source
        value: BigColumn  # more appropriate name here?
            vertex value
        **props: dictionary, optional
            vertex properties of the form property_name:property_value
            property_name is a string, and property_value is a literal value
            or a BigColumn source, which must be from same BigFrame as value arg

        Examples
        --------
        >>> movie_vertex = VertexRule('movie', f['movie'], genre=f['genre'])
        >>> user_vertex = VertexRule('user', f['user'], age=f['age_1'])
        """
        pass


class EdgeRule(Rule):
    def __init__(self, label, src, dst, is_directed=False, **props):
        """
        Specifies creation of an edge

        Parameters
        ----------
        label: str or BigColumn
            vertex label, static string or pulled from BigColumn source
        src: VertexRule
            source vertex; must be from same BigFrame as dst
        dst: VertexRule
            destination vertex; must be from same BigFrame as src
        is_directed : bool, optional
            indicates the edge is directed
        **props: dict, optional
            vertex properties of the form property_name:property_value
            property_name is a string, and property_value is a literal value
            or a BigColumn source, which must be from same BigFrame as src, dst

        Examples
        --------
        >>> rating_edge = EdgeRule('rating', movie_vertex, user_vertex, weight=f['score'])
        """
        pass


class PropertyRule(Rule):
    pass

class VertexPropertyRule(PropertyRule):
    def __init__(self, vertex, match_value, **props):
        """
        Specifies attachment of additional properties to a vertex

        Parameters
        ----------
        vertex : VertexRule
            target vertex for property attachment
        match_value: BigColumn  # more appropriate name?
            BigColumn source whose value must match against the value of the
            target vertex
        **props: dict, optional
            vertex properties of the form property_name:property_value
            property_name is a string, and property_value is a literal value
            or a BigColumn source

        Examples
        --------
        >>> extra_movie_rule = VertexPropertyRule(vertex_movie, f2['movie'], oscars=f2['oscars'])
        """
        pass


class EdgePropertyRule(PropertyRule):
    def __init__(self, edge, match_src, match_dst, **props):
        """
        Specifies attachment of additional properties to an edge

        Parameters
        ----------
        edge : EdgeRule
            target edge for property attachment
        match_src: BigColumn  # more appropriate name?
            BigColumn source whose value must match against the value of the
            target edge's source vertex
        match_dst: BigColumn  # more appropriate name?
            BigColumn source whose value must match against the value of the
            target edge's destination vertex
        **props: dict, optional
            edge properties of the form property_name:property_value
            property_name is a string, and property_value is a literal value
            or a BigColumn source

        Examples
        --------
        >>> extra_rating_rule = EdgePropertyRule(rating_edge, f2['movie'], f2['user'], rotten_tomatoes=f2['rt'])
        """
        pass


class GraphRule(Rule):
    def __init__(self, vid=None):
        """
        Specifies graph properties

        Parameters
        ----------
        vid : str, optional
            the name of the vertex id  # ie today's _gb_ID
        """
        pass


class BigGraph(object):
    def __init__(self, rules=None):
        """
        Creates a big graph

        Parameters
        ----------
        rules : list of Rule, optional
             list of rules which specify how the graph will be created

        Examples
        --------
        >>> g = BigGraph([user_vertex, movie_vertex, rating_edge, extra_movie_rule])
        """
        pass

    def add_props(self, rules):
        """
        Adds properties to existing vertices and edges

        Parameters
        ----------
        rules : list of PropertyRule
             list of property rules which specify how to add the properties

        Examples
        --------
        >>> g.add_props([extra_rating_rule])
        """
        pass


