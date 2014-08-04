from intelanalytics import *

dataset = r"datasets/apl.csv"
schema = [("user", int64), ("vertex_type", str), ("movie", int64), ("rating", int64), ("splits", str)]
csv_file = CsvFile(dataset, schema, skip_header_lines = 0)
print "Creating Frame"
f = BigFrame(csv_file)

user = VertexRule("user", f.user, { "vertex_type": "L"})
movie = VertexRule("movie", f.movie, { "vertex_type": "R"})
rates = EdgeRule("edge", user, movie, { "splits": f.splits,"rating":f.rating })
print("Creating Graph")
g = BigGraph([user, movie, rates] ,"apl")

print("Computing Average Path Length")
result = g.ml.average_path_length(input_edge_label_list = "edge", output_vertex_property_list = ["apl_num", "apl_sum"])
print(result)
