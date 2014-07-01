==============
Graph Database
==============

The Intel® Data Platform: Analytics Toolkit uses a graph database to handle complex, non-tabular data.
Graph databases are described as a series of vertices connected by edges.
We use these terms throughout our documentation.
Each vertex is a data element, like a field in a relational database.
The edges connect the vertices and show the relationships between them.
You might use this to perform organizational analysis, or to analyze business, or political relationships.

--------
Cloudera
--------

The graph database does not use indices and thus, does not use index lookups.
Each data element (vertex) has a pointer (edge) to the element adjacent to it.
This is great for handling graphs, and most Big Data problems are graph data.

See `Wikipedia\: Graph Data Structure`_.

Gremlin
-------

Gremlin_ is an open source, graph database, query language.
Think about it as SQL queries for graph databases.

.. _Wikipedia\: Graph Data Structure: http://en.wikipedia.org/wiki/Graph_data_structure
.. _Gremlin: https://github.com/tinkerpop/gremlin/wiki

