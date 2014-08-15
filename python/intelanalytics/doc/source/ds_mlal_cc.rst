    Connected Components (CC)
    = =======================

    The connected components algorithm finds all connected components in graph.
    The implementation is inspired by PEGASUS paper.

        Parameters
        - --------

        input_edge_label : String
            The edge property which contains the edge label.

        output_vertex_property_list : List (comma-separated string list)
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
            The default value is 1, which means output every super step.

        num_worker : String, optional
            The number of Giraph workers.
            The default value is 15.

    Returns


       output : AlgorithmReport
        The algorithm's results in the database.
        The progress curve is accessible through the report object.

    Example


        graph.ml.connected_components(
                    input_edge_label="connects",
                    output_vertex_property_list="component_id",
                    convergence_output_interval="1",
                    num_worker="3"
                    )


