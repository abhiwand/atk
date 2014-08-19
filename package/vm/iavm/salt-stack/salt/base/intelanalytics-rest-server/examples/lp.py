from intelanalytics import *

csv = CsvFile("datasets/lp_edge.csv", schema= [("source", int64), ("input_value", str), ("target", int64), ("weight", float64)], skip_header_lines=1)

print("Creating frame with name myframe1")

f = BigFrame(csv, "myframe1")

source = VertexRule("source", f.source, {"input_value" : f.input_value})

target = VertexRule("target", f.target, {"input_value" : f.input_value})

edge = EdgeRule("edge", source, target, {'weight': f.weight})

print("Creating graph with name mygraph1")

g = BigGraph([source, target, edge], "mygraph1")

print("Running Label Propagation on Graph mygraph1")

g.ml.label_propagation(vertex_value_property_list = "input_value", edge_value_property_list  = "weight", input_edge_label_list = "edge",   output_vertex_property_list = "lp_posterior",   vector_value = "true",    max_supersteps = 10,   convergence_threshold = 0.0, anchor_threshold = 0.9, lp_lambda = 0.5, bidirectional_check = False)

