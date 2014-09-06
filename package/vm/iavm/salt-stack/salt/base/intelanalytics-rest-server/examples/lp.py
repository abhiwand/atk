from intelanalytics import *

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/test_lda.csv"

#csv schema definition
schema = [("source", int64),
          ("input_value", str),
          ("target", int64),
          ("weight", float64)]

#csv schema definition
csv = CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame 'lp'"

frame = BigFrame(csv, "lp")

print "Done building data frame"

print "Inspecting frame 'lp'"

print frame.inspect()

source = VertexRule("source", frame.source, {"input_value" : frame.input_value})

target = VertexRule("target", frame.target, {"input_value" : frame.input_value})

edge = EdgeRule("edge", source, target, {'weight': frame.weight})

print "Creating graph 'lp_graph'"

graph = BigGraph([source, target, edge], "lp_graph")

print "Running Label Propagation on Graph 'lp_graph'"

print graph.ml.label_propagation(vertex_value_property_list="input_value",
                                 edge_value_property_list="weight",
                                 input_edge_label_list="edge",
                                 output_vertex_property_list="lp_posterior",
                                 vector_value="true",
                                 max_supersteps=10,
                                 convergence_threshold=0.0,
                                 anchor_threshold=0.9,
                                 lp_lambda=0.5,
                                 bidirectional_check=False)

