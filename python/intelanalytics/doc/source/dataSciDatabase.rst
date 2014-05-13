Graph Database
==============

The Intel Data Platform: Analytics Toolkit uses a graph database to handle complex, non-tabular data. Graph databases are described as a series of vertices connected by edges. We use these terms throughout our documentation. Each vertex is a data element, like a field in a relational database. The edges connect the vertices and show the relationships between them. You might use this to perform organizational analysis, or to analyze business, or political relationships.

Titan
-----

The Intel Data Platform: Analytics Toolkit uses the open source Titan Graph Database, from Aurelius. See http://thinkaurelius.github.io/titan/ for more details.

The Titan graph database does not use indices and thus, does not use index lookups. Each data element (vertex) has a pointer (edge) to the element adjacent to it. This is great for handling graphs, and most Big Data problems are graph data. See http://en.wikipedia.org/wiki/Graph_data_structure.

Graph Analytics
---------------

Graph analytics are the broad category of useful calculations you use to examine a graph. Examples of graph analytics may include traversals -- algorithmic walk throughs of the graph to determine optimal paths and relationship between vertices, and statistics -- that determine important attributes of the graph  such as degrees of separation, number of triangular counts, centralities (highly influential nodes), and so on. Some are user guided interactions, where the user navigates through the data connections, others are algorithmic, where a result is calculated by the software.

Graph learning is a class of graph analytics applying machine learning and data mining algorithms to graph data such that calculations are iterated across the nodes of the graph to uncover patterns and relationships, such as finding similarities based on relationships, or recursively optimizing some parameter across nodes.

Gremlin
-------

Gremlin is an open source, graph database, query language. Think about it as SQL queries for graph databases. See the official Gremlin page here: https://github.com/tinkerpop/gremlin/wiki. And the Titan Gremlin page here: https://github.com/thinkaurelius/titan/wiki/Gremlin-Query-Language.
