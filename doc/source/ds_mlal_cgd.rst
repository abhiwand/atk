Matrix Factorization based on Conjugate Gradient Descent (CGD)
==============================================================

This is the Conjugate Gradient Descent (CGD) with Bias for collaborative filtering algorithm.
Our implementation is based on the following paper:

Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative Filtering Model. In ACM KDD 2008. (Equation 5)
http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf

This algorithm for collaborative filtering is used in :term:`recommendation systems` to suggest items (products, movies, articles, and so on) to potential users based on historical records of items that all users have purchased, rated, or viewed.
The records are usually organized as a preference matrix P, which is a sparse matrix holding the preferences (such as, ratings) given by users to items.
Similar to ALS, CGD falls in the category of matrix factorization/latent factor model that infers user profiles and item profiles in low-dimension space, such that the original matrix P can be approximated by a linear model.

This factorization method uses the conjugate gradient method for its optimization subroutine. For more on
conjugate gradient descent in general, see: http://en.wikipedia.org/wiki/Conjugate_gradient_method.

The Mathematics of Matrix Factorization via CGD
-----------------------------------------------

Matrix factorization by conjugate gradient descent produces ratings by using the (limited) space of observed rankings to infer
a user-factors vector :math:`p_u` for each user  :math:`u`, and an item-factors vector :math:`q_i` for each item :math:`i`, and then producing
a ranking by user :math:`u` of item :math:`i` by the dot-product :math:`b_{ui} + p_u^Tq_i` where :math:`b_{ui}` is a baseline ranking
calculated as :math:`b_{ui} = \mu + b_u + b_i`.

The optimum model is chosen to minimum the following sum, which penalizes square distance of the prediction from observed rankings and complexity of the
model (through the regularization term):

.. math::
    \sum_{(u,i) \in {\mathcal{K}}} (r_{ui} - \mu - b_u - b_i - p_u^Tq_i)^2 + \lambda_3(||p_u||^2 + ||q_i||^2 + b_u^2 + b_i^2)    

Where:

    | :math:`r_{ui}` – Observed ranking of item :math:`i` by user :math:`u`
    | :math:`{\mathcal{K}}` – Set of pairs :math:`(u,i)` for each observed ranking of item :math:`i` by user :math:`u`
    | :math:`\mu` – The average rating over all ratings of all items by all users.
    | :math:`b_u` –  How much user :math:`u`'s average rating differs from :math:`\mu`.
    | :math:`b_i` –   How much item :math:`i`'s average rating differs from :math:`\mu`
    | :math:`p_u` –  User-factors vector.
    | :math:`q_i` – Item-factors vector.
    | :math:`\lambda_3` – A regularization parameter specified by the user.


This optimization problem is solved by the conjugate gradient descent method. Indeed, this difference in how the optimization problem is solved is the
primary difference between matrix factorization by CGD and matrix factorization by ALS.

Comparison between CGD and ALS
------------------------------

Both CGD and ALS provide recommendation systems based on matrix factorization; the difference is that
CGD employs the conjugate gradient descent instead of least squares for its optimization phase.
In particular, they share the same bipartite graph representation and the same cost function.

* ALS finds a better solution faster - when it can run on the cluster it is given.
* CGD has slighter memory requirements and can run on datasets that can overwhelm the ALS-based solution.

When feasible, ALS is a preferred solver over CGD, while CGD is recommended only when the application requires so much memory that it might be beyond the capacity of the system.  CGD has a smaller memory requirement, but has a slower rate of convergence and can provide a rougher estimate of the solution than the more computationally intensive ALS. 

The reason for this is that ALS solves the optimization problem by a least squares that requires inverting a matrix.
Therefore, it requires more memory and computational effort.
But ALS, a 2nd-order optimization method, enjoys higher convergence rate and is potentially more accurate in parameter estimation.

On the otherhand, CGD is a 1.5th-order optimization method that approximates the Hessian of the cost function from the previous gradient information through N consecutive CGD updates.
This is very important in cases where the solution has thousands or even millions of components.



Usage and Parameters
--------------------


Usage
~~~~~

The matrix factorization by CGD procedure takes a property graph, encoding a biparite user-item ranking network, selects a subset of the edges to be considered
(via a selection of edge labels), takes initial ratings from specified edge property values, and then writes each user-factors vector to its user vertex in a specified
vertex property name and each item-factors vector to its item vertex in the specified vertex property name.


Parameters
~~~~~~~~~~


    Required Parameters:

    edge_value_property_list : Comma Separated String
        The edge properties which contain the input edge values.
        We expect comma-separated list of property names  if you use
        more than one edge property.

    input_edge_label_list : Comma Separated String
        The labels of edge from the property graph used to provide the ranking.

    output_vertex_property_list : String List
        The list of vertex properties to store output vertex values. 

    vertex_type_property_key : String
        The name of the vertex property which contains vertex type for train/test/validate split.

    edge_type_property_key : String
        The name of edge property which contains edge type for train/test/validate split.


    Optional Parameters:

    vector_value: String
        True means a vector as vertex value is supported
        False means a vector as vertex value is not supported
        The default value is "false". When it is "true",  the output is written as a column separated list
        to the first given output_vertex_property_list. When false, the vectors are written by writing a component
        into each vertex property of output_vertex_property_list.


    max_supersteps : Integer 
        The maximum number of super steps (iterations) that the algorithm
        will execute.  The default value is 20.

    convergence_threshold : Float
        The amount of change in cost function that will be tolerated at convergence.
        If the change is less than this threshold, the algorithm exists earlier
        before it reaches the maximum number of super steps.
        The valid value range is all Float and zero.
        The default value is 0.

    cgd_lambda : Float 
        The tradeoff parameter that controls the strength of regularization (it is the parameter :math:`\lambda_3` in the equation above).
        Larger value implies stronger regularization that helps prevent overfitting
        but may cause the issue of underfitting if the value is too large.
        The value is usually determined by cross validation (CV).
        The valid value range is all positive Float and zero.
        The default value is 0.065.

    feature_dimension : Integer
        The length of feature vector to use in CGD model.
        Larger value in general results in more accurate parameter estimation,
        but slows down the computation.
        The valid value range is all positive integer.
        The default value is 3.

    learning_curve_output_interval : Integer 
        The learning curve output interval.
        Since each CGD iteration is composed by 2 super steps,
        the default one iteration means two super steps.

    bidirectional_check : Boolean 
        If it is True, Giraph will first check whether each edge is bidirectional
        before executing algorithm. CGD expects that each edge
        therefore should be bi-directional. This option is intended as sanity check.

    bias_on : Boolean
        True means turn on the update for bias term and False means turn off
        the update for bias term. Turning it on often yields more accurate model with
        minor performance penalty; turning it off disables term update and leaves the
        value of bias term to be zero.
        The default value is false.

    max_value : Float 
        The maximum edge weight value. If an edge weight is larger than this
        value, the algorithm will throw an exception and terminate. This option
        is mainly for graph integrity check.
        Valid value range is all Float.
        The default value is "Infinity".

    min_value : Float 
        The minimum edge weight value. If an edge weight is smaller than this
        value, the algorithm will throw an exception and terminate. This option
        is mainly for graph integrity check.
        Valid value range is all Float.
        The default value is "-Infinity".


Usage Example
~~~~~~~~~~~~~

    Graph.ml.cgd(
                input_edge_property_list="rating",
                input_edge_label_list="rates",
                output_vertex_property_list="cgd_results, cgd_bias",
                vertex_type="vertex_type",
                edge_type="edge_type",
                num_worker="3",
                max_supersteps="20",
                feature_dimension="3",
                cgd_lambda="0.065",
                convergence_threshold="0.001",
                learning_output_interval="1",
                max_val="10",
                min_val="1",
                bias_on="false",
                num_iters="3"
                )
