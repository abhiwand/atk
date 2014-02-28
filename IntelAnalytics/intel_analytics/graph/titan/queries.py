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
Common graph queries for convenience and common example use-cases.
"""
class GraphQueries(object):

    def __init__(self, graph):
        self.g = graph

    def vertex_lookup(self, **kwargs):
        """Looks up a single vertex given the properties/values supplied. 
        The properties used in the query must be indexes in the graph.

        Parameters
        ----------
        **kwargs: dict
            Keyword arguments for the vertex properties to query on.

        Returns
        -------
        output: Vertex
            The first vertex that matches the given criteria.

        Examples
        --------
        >>> q.vertex_lookup(term="Brain Cells")
        """
        return self.g.vertices.index.lookup(**kwargs).next()
    
    def mutual_edges(self, vid1, vid2):
        """Finds all the mutual edges between 2 vertices.

        Parameters
        ----------
        vid1: String
            Titan database vertex id.

        vid2: String
            Titan database vertex id.

        Returns
        -------
        output: list(dict)
            All edges with properties in a dictionary.
        """
        
        query_results = self.g.gremlin.execute("""
                        g.v(%s)
                        .bothE.as('e')
                        .bothV
                        .retain([g.v(%s)])
                        .back('e')
                    """ % (vid1, vid2)).results      
        for e in query_results:
            yield e.data
            
    def top_edges(self, vid, edge_weight_property, edge_label=None, top_n=10):
        """Finds the top_n edges connected to a given vertex (vid), ranked by edge_weight_property.
        
        Parameters
        ----------
        vid: String
            Titan database vertex id.

        edge_weight_property: String
            Edge property to weight the edges by to determine how to rank them.

        edge_label: String
            Edge label to restrict the result set to.

        top_n: int 
            Maximum number of edges to return

        Returns
        -------
        output: list(dict)
            List of edges with all properties in a dictionary.
        """
        
        query = """
            g.v(%(vid)s)
             .bothE %(edge_label)s
             .order{it.b['%(edge_weight_property)s'] <=> it.a['%(edge_weight_property)s']}[0..<%(top_n)d]
        """ % {'vid': vid, 
               'edge_label': "('" + edge_label + "')" if edge_label is not None else '',
               'top_n': top_n,
               'edge_weight_property': edge_weight_property} 
        
        for e in self.g.gremlin.execute(query).results:
            yield e.data
            
    def top_vertices(self, vid, edge_weight_property, edge_label=None, top_n=10):
        """Finds the top_n vertices connected to a given vertex (vid), ranked by edge_weight_property.
            Optionally filters by the given edge label.

        Parameters
        ----------
        vid: String
            Titan database vertex id.

        edge_weight_property: String
            Edge property to weight the edges by to determine how to rank them.

        edge_label: String
            Edge label to restrict the result set to.

        top_n: int 
            Maximum number of edges to return

        Returns
        -------
        output: list(dict)
            List of vertices with all properties in a dictionary.
        """
        
        query = """
            g.v(%(vid)s).as('v0')
             .bothE %(edge_label)s
             .order{it.b['%(edge_weight_property)s'] <=> it.a['%(edge_weight_property)s']}[0..<%(top_n)d]
             .bothV.except('v0')
             .dedup()
        """ % {'vid': vid, 
               'edge_label': "('" + edge_label + "')" if edge_label is not None else '',
               'top_n': top_n,
               'edge_weight_property': edge_weight_property} 
        
        for e in self.g.gremlin.execute(query).results:
            yield e.data
            
    def top_vertices_with_all_edges(self, vid, edge_weight_property, edge_label=None, top_n=10):
        """Finds the top_n vertices and all connected edges from a given vertex (vid), ranked by
            edge_weight_property. Optionally filters by the given edge.

        Parameters
        ----------
        vid: String
            Titan database vertex id.

        edge_weight_property: String
            Edge property to weight the edges by to determine how to rank them.

        edge_label: String
            Edge label to restrict the result set to.

        top_n: int 
            Maximum number of edges to return

        Returns
        -------
        output: list(dict('vertex':dict, 'edge':dict))
            List of vertex/edge pairs with all properties of each object in a dictionary.
        """
        
        query = """
            g.v(%(vid)s).as('v0')
                .bothE %(edge_label)s
                .order{it.b['%(edge_weight_property)s'] <=> it.a['%(edge_weight_property)s']}[0..<%(top_n)d].as('e0')
                .bothV.except('v0').as('vertex')
                .bothE.as('edge')
                .bothV.retain('v0')
                .select(['vertex', 'edge'])
        """ % {'vid': vid, 
               'edge_label': "('" + edge_label + "')" if edge_label is not None else '',
               'top_n': top_n,
               'edge_weight_property': edge_weight_property}
        
        for e in self.g.gremlin.execute(query).results:
            yield e.data
            
    def one_hop(self, vids, edge_label=None, rank_by_edge_property=None, max_edges=100):
        """Gets all the vertices and edges within one hop of the given vertices (vids).

        Parameters
        ----------
        vids: list(String)
            Titan database vertex ids.

        edge_label: String
            Edge label to restrict the result set to.

        rank_by_edge_property: String
            Edge property to consider for ranking edges. 
            This is used in conjuction with max_edges so that the most important edges are kept in the result set.

        max_edges: int 
            Maximum number of edges to return

        Returns
        -------
        output: list(dict('edge':dict, 'v1':dict, 'v2':dict))
            List of edges with connected vertices. Each edge/vertex is a dictionary of all its properties.

        Examples
        --------
        >>> list(q.one_hop([3924002775846879332, 3194978059270750232, 7449653313584234680], 
                            '2012', rank_by_edge_property='etl-cf:count', max_edges=20))
        """
        
        query = """
            v0 = [%(vids)s];
            v0._().bothE %(edge_label)s %(orderedges)s[0..<%(top_n)d]
            .bothV.dedup.as('v1')
            .outE %(edge_label)s 
            .as('edge')
            .inV.retain(v0).as('v2')
            .select(['v1','v2','edge'])   
        """ % {'vids': ', '.join(["g.v(%s)" % (v) for v in vids]),
               'edge_label': "('" + edge_label + "')" if edge_label is not None else '',
               'orderedges': ".order{it.b['%(x)s'] <=> it.a['%(x)s']}" % {'x':rank_by_edge_property} if rank_by_edge_property is not None else '',
               'top_n': max_edges}

        for e in self.g.gremlin.execute(query).results:
            yield e.data
            
    def all_edges(self, vids, edge_label=None, rank_by_edge_property=None, max_edges=100):
        """Gets all the mutual edges for the set of provided vertices (vids).

        Parameters
        ----------
        vids: list(String)
            Titan database vertex ids.

        edge_label: String
            Edge label to restrict the result set to.

        rank_by_edge_property: String
            Edge property to consider for ranking edges. 
            This is used in conjuction with max_edges so that the most important edges are kept in the result set.

        max_edges: int 
            Maximum number of edges to return

        Returns
        -------
        output: list(dict('edge':dict, 'v1':dict, 'v2':dict))
            List of edges with connected vertices. Each edge/vertex is a dictionary of all its properties.

        Examples
        --------
        >>> list(q.all_edges([3924002775846879332, 3194978059270750232, 7449653313584234680], 
                            '2012', rank_by_edge_property='etl-cf:count', max_edges=2))
        """
        
        query = """
            v0 = [%(vids)s];
            v0._()
            .outE %(edge_label)s
            .as('e0')
            .inV.retain(v0)
            .back('e0')%(orderedges)s[0..<%(top_n)d]
        """ % {'vids': ', '.join(["g.v(%s)" % (v) for v in vids]),
               'edge_label': "('" + edge_label + "')" if edge_label is not None else '',
               'orderedges': ".order{it.b['%(x)s'] <=> it.a['%(x)s']}" % {'x':rank_by_edge_property} if rank_by_edge_property is not None else '',
               'top_n': max_edges}

        for e in self.g.gremlin.execute(query).results:
            yield e.data
    