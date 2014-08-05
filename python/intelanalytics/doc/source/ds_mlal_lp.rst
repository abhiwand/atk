Label Propagation (LP)
======================

Label propagation (LP) is a message passing technique for inputing or smoothing labels in partially-labelled datasets. 
Labels are propagated from *labeled* data to *unlabeled* data along a graph encoding similarity relationships among data points.
The labels of known data can be probabilistic 
in other words, a known point can be represented with fuzzy labels such as 90% label 0 and 10% label 1.
The inverse distance between data points is represented by edge weights, with closer points having a higher weight (stronger influence
on posterior estimates) than points farther away. 
LP has been used for many problems, particularly those involving a similarity measure between data points.
Our implementation is based on Zhu and Ghahramani's 2002 paper, "Learning from labeled and unlabeled data" [1]_.
  
The Label Propagation Algorithm:
--------------------------------
     
In LP, all nodes start with a prior distribution of states and the initial messages vertices pass to their neighbors are simply their prior beliefs. 
If certain observations have states that are known deterministically, they can be given a prior probability of 100% for their true state and 0% for 
all others.
Unknown observations should be given uninformative priors.
    
Each node, :math:`i`, receives messages from their :math:`k` neighbors and updates their beliefs by taking a weighted average of their current beliefs
and a weighted average of the messages received from its neighbors.
    
The updated beliefs for node :math:`i` are:

.. math::

    updated\ beliefs_{i} = \lambda * (prior\ belief_{i} ) + (1 - \lambda ) * \sum_k w_{i,k} * previous\ belief_{k}

where :math:`w_{i,k}` is the normalized weight between nodes :math:`i` and :math:`k` such that the sum of all weights to neighbors is one,
and :math:`\lambda` is a learning parameter.
If :math:`\lambda` is greater than zero, updated probabilities will be anchored in the direction of prior beliefs.
The final distribution of state probabilities will also tend to be biased in the direction of the distribution of initial beliefs. 
For the first iteration of updates, nodes' previous beliefs are equal to the priors, and, in each future iteration,
previous beliefs are equal to their beliefs as of the last iteration.
All beliefs for every node will be updated in this fashion, including known observations, unless ``anchor_threshold`` is set.
The ``anchor_threshold`` parameter specifies a probability threshold above which beliefs should no longer be updated. 
Hence, with an ``anchor_threshold`` of 0.99, observations with states known with 100% certainty will not be updated by this algorithm.

This process of updating and message passing continues until the convergence criteria is met, or the maximum number of super steps is reached.
A node is said to converge if the total change in its cost function is below the convergence threshold.
The cost function for a node is given by:

.. math::

    cost = \sum_k w_{i,k} * \left [ \left ( 1 - \lambda \right ) * \left [ previous\ belief_{i}^{2} - w_{i,k} * previous\ belief_{i} * previous\
    belief_{k} \right ] + 0.5 * \lambda * \left ( previous\ belief_{i} - prior_{i} \right ) ^{2} \right ]

Convergence is a local phenomenon; not all nodes will converge at the same time. 
It is also possible for some (most) nodes to converge and others to never converge. 
The algorithm requires all nodes to converge before declaring that the algorithm has converged overall. 
If this condition is not met, the algorithm will continue up to the maximum number of super steps.

