from intelanalytics import *

dataset = r"datasets/netflix.csv"

schema = [("user_id", int32), ("direction", str), ("movie_id", int32), ("rating", int32), ("splits", str)]

csv_file = CsvFile(dataset, schema, skip_header_lines = 0)

print("Creating DataFrame")

f = BigFrame(csv_file)

user = VertexRule("user_id", f.user_id, { "vertex_type": "L"})

movie = VertexRule("movie_id", f.movie_id, { "vertex_type": "R"})

rates = EdgeRule("edge", user, movie, { "splits": f.splits,"rating":f.rating })

print("Creating Graph als")

g = BigGraph([user, movie, rates] ,"als")

print("Running Alternating Least Squares on Graph als")

g.ml.alternating_least_squares(edge_value_property_list = "rating", vertex_type_property_key = "vertex_type", input_edge_label_list = "edge", output_vertex_property_list = "als_result ", edge_type_property_key = "splits",vector_value = "true",als_lambda = 0.065)
