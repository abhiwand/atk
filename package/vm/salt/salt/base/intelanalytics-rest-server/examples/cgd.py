from intelanalytics import *

dataset = r"datasets/movie_sample_data_5mb.csv"

schema = [("user_id", int32), ("direction", str), ("movie_id", int32), ("rating", int32), ("splits", str)]

csv_file = CsvFile(dataset, schema, skip_header_lines = 0)

print("Creating DataFrame")

f = BigFrame(csv_file)

user = VertexRule("user_id", f.user_id, { "vertex_type": "L"})

movie = VertexRule("movie_id", f.movie_id, { "vertex_type": "R"})

rates = EdgeRule("edge", user, movie, { "splits": f.splits,"rating":f.rating })

print("Creating Graph cgd")

g = BigGraph([user, movie, rates] ,"cgd")

print("Running Conjugate Gradient Descent on Graph cgd")

g.ml.conjugate_gradient_descent(edge_value_property_list = "rating", vertex_type_property_key = "vertex_type", input_edge_label_list = "edge", output_vertex_property_list = "cgd_result ", edge_type_property_key = "splits",vector_value = "true",cgd_lambda = 0.065, num_iters = 3)
