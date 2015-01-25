Trained model recommendation.

Get recommendation to either left-side or right-side vertices.
The prerequisite is at least one of two algorithms (ALS or CGD) has
been run before this query.

Parameters
----------
vertex_id : string
    The vertex id to get recommendation for

vertex_type : string
    The vertex type to get recommendation for.
    The valid value is either "L" or "R".
    "L" stands for left-side vertices of a bipartite graph.
    "R" stands for right-side vertices of a bipartite graph.
    For example, if your input data is "user,movie,rating" and
    you want to get recommendations on user, input "L" because
    user is your left-side vertex.
    Similarly, input "R" if you want to get recommendations for movie.

left_vertex_id_property_key : string
    The property name for left side vertex id.

right_vertex_id_property_key : string
    The property name for right side vertex id.

output_vertex_property_list : comma-separated string (optional)
    The property name for ALS/CGD results.
    When bias is enabled,
    the last property name in the output_vertex_property_list is for bias.
    The default value is "als_result".

vertex_type_property_key : string (optional)
    The property name for vertex type.
    The default value is "vertex_type".

edge_type_property_key : string (optional)
    The property name for edge type.
    We need this name to know data is in train, validation or test splits.
    The default value is "splits".

vector_value : string (optional)
    Whether ALS/CDG results are saved in a vector for each vertex.
    The default value is "true".

bias_on : string (optional)
    Whether bias turned on/off for ALS/CDG calculation.
    When bias is enabled,
    the last property name in the output_vertex_property_list is for bias.
    The default value is "false".

train_str : string (optional)
    The label for training data.
    The default value is "TR".

num_output_results : int (optional)
    The number of recommendations to output.
    The default value is 10.

left_vertex_name : string (optional)
    The real name for left side vertex.

right_vertex_name : string (optional)
    The real name for right side vertex.

Returns
-------
List of rank and corresponding recommendation
    Recommendations for the input vertex

Examples
--------
.. only:: html

    For example, if your right-side vertices are users,
    and you want to get movie recommendations for user 8941,
    the command to use is::

        g.query.recommend(right_vertex_id_property_key='user', left_vertex_id_property_key='movie_name', vertex_type="R", vertex_id = "8941")

    The expected output of recommended movies looks like this::

        {u'recommendation': [{u'vertex_id': u'once_upon_a_time_in_mexico', u'score': 3.831419911100037, u'rank': 1},{u'vertex_id': u'nocturne_1946', u'score': 3.541907655192171, u'rank': 2},{u'vertex_id': u'red_hot_skate_rock', u'score': 3.2573571020389407, u'rank': 3}]}

.. only:: latex

    For example, if your right-side vertices are users,
    and you want to get movie recommendations for user 8941,
    the command to use is::

        g.query.recommend( \\
            right_vertex_id_property_key='user', \\
            left_vertex_id_property_key='movie_name', \\
            vertex_type="R", \\
            vertex_id = "8941")

    The expected output of recommended movies looks like this::

        {u'recommendation': [{u'vertex_id': u'once_upon_a_time_in_mexico',
        u'score': 3.831419911100037, u'rank': 1},
        {u'vertex_id': u'nocturne_1946', u'score': 3.541907655192171, u'rank': 2},
        {u'vertex_id': u'red_hot_skate_rock', u'score': 3.2573571020389407,
        u'rank': 3}]}


