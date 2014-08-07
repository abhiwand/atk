-----------------------
Collaborative Filtering
-----------------------

Collaborative filtering is a technique that is widely used in recommendation systems to suggest items (for example, products, movies,
articles) to potential users based on historical records of items that users have purchased, rated, or viewed.
The Intel Analytics Toolkit provides two different implementations of collaborative filtering, which differ only in their optimization method,
either Alternative Least Squares (ALS) or Conjugate Gradient Descent (CGD).
Both methods optimize the cost function found in Y. Koren,
`Factorization Meets the Neighborhood\: a Multifaceted Collaborative Filtering Model`_
in ACM KDD 2008.
For more information on optimizing using ALS see, Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan,
`Large-Scale Parallel Collaborative Filtering for the Netflix Prize`_ , 2008.
CGD provides a faster, more approximate optimization of the cost function and should be used when memory is a constraint.

A typical representation of the preference matrix P in Giraph is a bi-partite graph, where nodes at the left side represent a list of
users and nodes at the right side represent a set of items (for example, movies), and edges encode the rating a user provided to an item.
To support training, validation and test, a common practice in machine learning, each edge is also annotated by “TR”, “VA” or “TE”.  

.. image:: ds_mlal_cf_1.png
   :align: center
   :width: 80 %

Each node in the graph will be associated with a vector :math:`\textstyle \overrightarrow {f_x}` of length :math:`k`, where :math:`k`
is the feature dimension specified by the user, and a bias term :math:`b_x`.
ALS optimizes :math:`\textstyle \overrightarrow {f_x}` and :math:`b_x` alternatively between user profiles using least
squares on users and on items.
At each step, the total prediction error for each item or user is computed and the bias term for that item or user is recomputed for use in
the next iteration:

.. math::

    b = \frac {\sum error}{(1 + \lambda) * n}

At each step, the regularized cost function that is minimized is:

.. math::

    cost = \frac {\sum error^2} {n} + \lambda * \left( bias^2 + \sum f_k^2 \right)

Note that the equations above omit user and item subscripts for generality.
The regularization term, :math:`\lambda`, tries to avoid overfitting by penalizing the magnitudes of the parameters.
After the parameters :math:`\textstyle \overrightarrow {f_x}` and :math:`b_x` are determined, given an item :math:`m_j`,
the rating from user :math:`u_i` can be predicted by a simple linear model:

.. math::

    r_{ij} = \overrightarrow {f_{ui}} \cdot \overrightarrow {f_{mj}} + b_{ui} + b_{mj}


.. _Factorization Meets the Neighborhood\: a Multifaceted Collaborative Filtering Model: http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf
.. _Large-Scale Parallel Collaborative Filtering for the Netflix Prize: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.173.2797
