Page Rank (PR)
==============

Basics and Background
---------------------

*PageRank* is a method for determining which vertices in a directed graph are the most central or important.
*PageRank* gives each vertex a score which can be interpreted as the probability that a person randomly walking along the edges
of the graph will visit that vertex.

The calculation of *PageRank* is based on the supposition that if a vertex has many vertices pointing to it, then it is “important”,
and that a vertex grows in importance as more important vertices point to it.
The calculation is based only on the network structure of the graph and makes no use of any side data, properties, user-provided scores or
similar non-topological information.

*PageRank* was most famously used as the core of the Google search engine for many years, but as a general measure of
:term:`centrality` in a graph, it has other uses to other problems, such as :term:`recommendation systems` and analyzing predator-prey
food webs to predict extinctions.

Background references:
~~~~~~~~~~~~~~~~~~~~~~

*   Basic description and principles: `Wikipedia\: PageRank`_
*   Applications to food web analysis: `Stanford\: Applications of PageRank`_
*   Applications to recommendation systems: `PLoS\: Computational Biology`_

Mathematical Details of PageRank Implementation
-----------------------------------------------

Our implementation of *PageRank* satisfies the following equation at each vertex :math:`v` of the graph:

.. math::

    PR(v) = \frac {\rho}{n} + \rho \left (\sum_{u\in InSet(v)} \frac {PR(u)}{L(u)} \right ) 

Where:

| :math:`\vartriangleright v` – a vertex
| :math:`\vartriangleright L(v)` – outbound degree of the vertex v
| :math:`\vartriangleright PR(v)` – *PageRank* score of the vertex v 
| :math:`\vartriangleright InSet(v)` – set of vertices pointing to the vertex v 
| :math:`\vartriangleright n` – total number of vertices in the graph
| :math:`\vartriangleright \rho`- user specified damping factor (also known as reset probability)

Termination is guaranteed by two mechanisms.

*   The user can specify a convergence threshold so that the algorithm will terminate when at every vertex,
    the difference between successive approximations to the *PageRank* score falls below the convergence threshold.

*   The user can specify a maximum number of iterations after which the algorithm will terminate.

Usage and Parameters
--------------------

Usage
~~~~~

The *PageRank* analysis takes a property graph, a sub-selection of the edges of the property to provide the link structure for
the analysis (specified by a list of desired edge labels),  performs the analysis and updates the graph in the database with
a new property at each vertex that contains that vertex’s *PageRank* score.
The name of the new property is specified by the caller.

Parameters::

    input_edge_label_list *:*
        The label of the edges used to generate the graph for *PageRank* Analysis.

    output_vertex_property_list *:* List (comma-separated list of strings)
        The name of the vertex property used to store the *PageRank* score of the vertex.

    max_supersteps *:* int (optional)
        The number of iterations to run the *PageRank* algorithm.
        The default value is 20.

    convergence_threshold *:* double (optional)
        The convergence threshold which controls how small the change in belief value must be in order to meet the convergence criteria.
        The default value is 0.001.

    reset_probability *:* double (optional)
        The damping factor :math:`\rho` in the equation of the preceding subsection.
        The default value is 0.15.

    convergence_output_interval *:* int (optional)
        Controls how frequently a progress report is logged on the server.
        The convergence progress output interval.
        The default value is 1, which means output every super step.

Example Usage::

    graph.ml.page_rank(
                        self,
                        input_edge_label=”edges”,
                        output_vertex_property_list=”page_rank”,
                        max_supersteps=20,
                        convergence_threshold=0.001,
                        reset_probability=0.15,
                        convergence_output_interval=1
                        )



.. _Wikipedia\: PageRank: http://en.wikipedia.org/wiki/PageRank
.. _Stanford\: Applications of PageRank: http://web.stanford.edu/class/msande233/handouts/lecture8.pdf
.. _PLoS\: Computational Biology:
    http://www.ploscompbiol.org/article/fetchObject.action?uri=info%3Adoi%2F10.1371%2Fjournal.pcbi.1000494&representation=PDF
