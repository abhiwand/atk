#!/usr/bin/python2.7
import intelanalytics as ia

ia.connect();

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/lp_edge.csv"

#csv schema definition
schema = [("source", ia.int64),
          ("input_value", str),
          ("target", ia.int64),
          ("weight", ia.float64)]

#csv schema definition
csv = ia.CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame 'lp'"

frame = ia.Frame(csv, "lp")

print "Done building data frame"

print "Inspecting frame 'lp'"

print frame.inspect()

source = ia.VertexRule("source", frame.source, {"input_value" : frame.input_value})

target = ia.VertexRule("target", frame.target, {"input_value" : frame.input_value})

edge = ia.EdgeRule("edge", source, target, {'weight': frame.weight}, bidirectional=True)

print "Creating graph 'lp_graph'"

graph = ia.TitanGraph([source, target, edge], "lp_graph")

print "Running Label Propagation on Graph 'lp_graph'"

print graph.ml.label_propagation(vertex_value_property_list = ["input_value"],
                                 edge_value_property_list = ["weight"],
                                 input_edge_label_list = ["edge"],
                                 output_vertex_property_list = ["lp_posterior"],
                                 vector_value = True,
                                 max_supersteps = 10,
                                 convergence_threshold = 0.0,
                                 anchor_threshold = 0.9,
                                 lp_lambda = 0.5,
                                )

