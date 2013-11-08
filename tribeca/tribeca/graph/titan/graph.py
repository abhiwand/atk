"""
Titan-specific implementation
"""

from tribeca.graph.titan.ml import TitanGiraphMachineLearning
from tribeca.graph.biggraph import GraphBuilder, GraphBuilderFactory

class TitanGraph(object):   # TODO: inherit BigGraph
    """
    proxy for a graph stored in Titan
    """

    def __init__(self):
        self.ml = TitanGiraphMachineLearning(self)


#------------------------------------------------------------------------------
# Graph Builder
#------------------------------------------------------------------------------
class TitanGraphBuilderFactory(GraphBuilderFactory):
    """
    Provides a set of graph builders for a particular table
    """

    def __init__(self, table):
        self._table = table

    def get_bipartite_graph_builder(self):
        return TitanBipartiteGraphBuilder(self._table)

    def get_property_graph_builder(self):
        return TitanPropertyGraphBuilder(self._table)


class TitanBipartiteGraphBuilder(GraphBuilder):
    """
    bipartite graph builder for HBase->Titan
    """
    def __init__(self, table): # TBD all that these GraphBuilders need
                               # HBase and Titan parameters, etc.
        self._table = table

    def register_vertex(col_name):
        """
        col_name : string
            vertex column name
        """
        pass

    def register_vertices(col_names):
        """
        col_names : list of strings
            column names for the vertices
        """
        pass

    def build(self):
        return TitanGraph()



class TitanPropertyGraphBuilder(GraphBuilder):
    """
    property graph builder for HBase->Titan
    """
    def __init__(self, table):
        self._table = table  # HBaseTable

    def register_vertex(col_name, properties):
        """
        col_name : string
            vertex column name

        properties : list of properties
            column names for the vertex properties
        """
        pass

    def register_vertices(col_names):
        """
        verts : dict of string -> list of strings
            kvp's to call register_vertex multiple times
        """
        pass

    def register_edge(start_col_name, end_col_name, label, properties):
        """
        start_col_name : string
            starting vertex
        end_col_name : string
            ending vertex
        label : string
            column name for edge label
        properties : list of strings
            column names for the properties
        """
        pass

    def register_edges(edges):
        """
        edges : TBD, dict or list of tuples...
        """
        pass

    def build(self):
        return TitanGraph()


