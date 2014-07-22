==============
Graph Database
==============

The Intel® Analytics Toolkit uses a :term:`graph` database to handle complex, non-tabular data.
:term:`Graph` databases are described as a series of vertices connected by edges.
Each :term:`vertex` is a data element, like a field in a tabular database.
Each data element (:term:`vertex`) has a pointer (:term:`edge`) to the element adjacent to it.
An :term:`edge` connects the vertices and shows the relationships between them.
You might use a :term:`graph` database to perform organizational analysis, or to analyze business, or political relationships.
Most Big Data problems are handled as :term:`graph` data.

Note:
    A :term:`graph` database does not use indices and thus, does not use index lookups like tabular databases.

See `Wikipedia\: Graph Data Structure`_.

Gremlin
-------

:term:`Gremlin` is an open source, :term:`graph` database, query language.
Think about it as SQL queries for :term:`graph` databases.

.. _Wikipedia\: Graph Data Structure: http://en.wikipedia.org/wiki/Graph_data_structure

