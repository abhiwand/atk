==============
Graph Sampling
==============

Analyzing a massive :term:`graph` with millions or billions of nodes is a time-intensive, hence, expensive process.
Often analysis is done on a subset of the data.
Sampling is the process whereby a subset of the data is obtained.

Traditional data sampling methods, such as :term:`simple random sampling` and :term:`stratified sampling`, assume that the data is
independent and identically distributed (IID.)
Although this assumption does not always hold for non-relational data, the very nature of relational data means that
any assumption of IID data can not be made.

A simple example where the IID assumption is invalid can be found with social network data.
Consider a group of individuals that each have a favorite music attribute.
There has been a tremendous amount of research in social psychology supporting the idea that members of social groups tend to influence each other.
For example, social groups might form similar music preferences.
Thus, the relationships among individuals can have a significant impact on the attributes of the individuals.

In order to deal with the potential dependencies between individual attributes, a variety of sampling techniques have been proposed
for relational (or :term:`graph`) data.
These techniques are categorized as either node-based, edge-based, or exploration-based sampling.

Node-based sampling is a relational data sampling technique that is most similar to the traditional non-relational data sampling techniques.

Although uniform-weighted node sampling is often used for certain types of data, there are other types of weighted node sampling.
Degree-weighted node sampling preserves the degree distribution of a :term:`graph` better than uniform-weighted node sampling.
Similarly, :term:`PageRank`-weighted node sampling can be used if we are concerned about preserving personal :term:`PageRank` values.

Edge-based sampling is not often used due to bias that it introduces toward high-degree nodes.
Naturally, the number of edges incident on a node increases as its degree increases.
Therefore, a random selection of edges is more likely to sample edges that are incident on high-degree nodes.

Exploration-based sampling is useful in cases where we want to best preserve common structural properties of a
:term:`graph` (for example: degree distribution, clustering coefficient, density).
Random Walk, Random Jump, and Forest Fire have been used to sample :term:`graph` data with good results.

.. TODO::
    
    Add references that show good results from these types of sampling.

Application of each algorithm and its variants (for example, Metropolis-Hasting Random Walk) should take into account the
properties of the :term:`graph` being sampled.
For a simple :term:`graph` without any meaningful attributes, a weighted node sampling algorithm might be sufficient.
The Forest Fire algorithm was developed to preserve the power-law properties of such a :term:`graph`.
Knowledge of the :term:`graph` properties will help to determine the most appropriate :term:`graph` sampling algorithm.

It is worth noting that the inclusion of multiple types of edges further complicates :term:`graph` sampling techniques.
Each different type of :term:`edge` may induce a very different :term:`graph` structure.
Therefore, it must be specified what type of :term:`edge` is of interest when applying the aforementioned sampling algorithms.

