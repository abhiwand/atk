    Average Path Length (APL)
    = =======================

    The :term:`average path length` algorithm calculates the average path length from a vertex to any other vertices.

        Parameters
        - --------

        input_edge_label : String
            The edge property which contains the edge label.

        output_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain the output vertex values.
            If you use more than one vertex property, we expect a comma-separated string list.

        num_mapper : String, optional
            A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum.
            Use on the fly when needed for your data sets.

        mapper_memory : String, optional
            A reconfigured Hadoop parameter mapred.map.child.java.opts.
            Use on the fly when needed for your data sets.

        convergence_output_interval : String, optional
            The convergence progress output interval.
            The default value is 1, which means output every :term:`superstep`.

        num_worker : String, optional
            The number of Giraph workers.
            The default value is 15.

    Returns


    Output : AlgorithmReport

            The algorith's results in the database.
            The progress curve is accessible through the report object.

    Example


        graph.ml.avg_path_len(
                    input_edge_label="edge",
                    output_vertex_property_list="apl_num, apl_sum",
                    convergence_output_interval="1",
                    num_worker="3"
                    )


