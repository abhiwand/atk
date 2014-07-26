from intelanalytics import *
dataset = r"netflix.csv"
schema = [("user_id", int32), ("movie_id", int32), ("rating", int32), ("splits", str)]
csv_file = CsvFile(dataset, schema, skip_header_lines = 0)
f = BigFrame(csv_file)
user = VertexRule("user_id", f.user_id, { "vertex_type": "L"})
movie = VertexRule("movie_id", f.movie_id, { "vertex_type": "R"})
rates = EdgeRule("edge", user, movie, { "splits": f.splits,"rating":f.rating })
g = BigGraph([user, movie, rates] ,"connected_components_demo_graph")
g.ml.connected_components(input_edge_label = "edge", output_vertex_property = "component_id")
