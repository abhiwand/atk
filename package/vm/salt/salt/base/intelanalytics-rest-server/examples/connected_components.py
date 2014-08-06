from intelanalytics import *

dataset = r"netflix.csv"
schema = [("user_id", int64), ("movie_id", int64), ("rating", int64), ("splits", str)]
csv_file = CsvFile(dataset, schema, skip_header_lines = 0)

print("Creating Frame")
f = BigFrame(csv_file)

user = VertexRule("user_id", f.user_id, { "vertex_type": "L"})
movie = VertexRule("movie_id", f.movie_id, { "vertex_type": "R"})
rates = EdgeRule("edge", user, movie, { "splits": f.splits,"rating":f.rating })

print("Creating Graph connected_components_demo")
g = BigGraph([user, movie, rates] ,"connected_components_demo")

print("Running Connected Components Algorithm")
result = g.ml.connected_components(input_edge_label = "edge", output_vertex_property = "component_id")

print(result)
