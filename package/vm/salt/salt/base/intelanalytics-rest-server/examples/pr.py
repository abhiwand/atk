from intelanalytics import *

dataset = r"datasets/movie_data_random.csv"

schema = [("user_id", int32), ("movie_id", int32), ("rating", int32), ("splits", str)]

csv_file = CsvFile(dataset, schema, skip_header_lines = 1)

print("Creating DataFrame")
f = BigFrame(csv_file)

user = VertexRule("user_id", f.user_id, {"vertex_type": "L"})

movie = VertexRule("movie_id", f.movie_id, {"vertex_type": "R"})

rates = EdgeRule("edge", user, movie, {"splits": f.splits, "rating": f.rating})

print("Creating graph pr")

print("Creating graph pr")
g = BigGraph([user, movie, rates], "pr")


g.ml.page_rank(input_edge_label_list = "edge", output_vertex_property_list = "pr_result")
