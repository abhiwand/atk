from intelanalytics import *

csv = CsvFile("datasets/lbp_edge.csv", schema= [("source", int64), ("value", str), ("vertex_type", str), ("target", int64) , ("weight", float64)], skip_header_lines=1)

print("Creating DataFrame myframe")

f = BigFrame(csv, "myframe")

source = VertexRule("source", f.source, {"vertex_type" : f.vertex_type, "value" : f.value})

target = VertexRule("target", f.target, {"vertex_type" : f.vertex_type, "value" : f.value})

edge = EdgeRule("edge", target, source, {'weight': f.weight})

print("Creating Graph mygraph")

g = BigGraph([target, source, edge], "mygraph")

print("Running Loopy Belief Propagation on Graph mygraph")

g.ml.loopy_belief_propagation(vertex_value_property_list = "value", edge_value_property_list  = "weight", input_edge_label_list = "edge",   output_vertex_property_list = "lbp_posterior",   vertex_type_property_key = "vertex_type",  vector_value = "true",    max_supersteps = 10,   convergence_threshold = 0.0, anchor_threshold = 0.9, smoothing = 2.0, bidirectional_check = False,  ignore_vertex_type = False, max_product= False, power = 0)
 
