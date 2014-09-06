from intelanalytics import *

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/lbp_edge.csv"

#csv schema definition
schema = [("source", int64),
          ("value", str),
          ("vertex_type", str),
          ("target", int64),
          ("weight", float64)]

csv = CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame 'myframe'"

frame = BigFrame(csv, "myframe")

print "Done building data frame 'myframe'"

print "Inspecting frame 'myframe'"

print frame.inspect()

source = VertexRule("source", frame.source, {"vertex_type": frame.vertex_type, "value": frame.value})

target = VertexRule("target", frame.target, {"vertex_type": frame.vertex_type, "value": frame.value})

edge = EdgeRule("edge", target, source, {'weight': frame.weight})

print "Creating Graph 'mygraph'"

graph = BigGraph([target, source, edge], "mygraph")

print "Running Loopy Belief Propagation on Graph mygraph"

print graph.ml.loopy_belief_propagation(vertex_value_property_list="value",
                                        edge_value_property_list="weight",
                                        input_edge_label_list="edge",
                                        output_vertex_property_list="lbp_posterior",
                                        vertex_type_property_key="vertex_type",
                                        vector_value="true",
                                        max_supersteps=10,
                                        convergence_threshold=0.0,
                                        anchor_threshold=0.9,
                                        smoothing=2.0,
                                        bidirectional_check=False,
                                        ignore_vertex_type=False,
                                        max_product=False,
                                        power=0)
