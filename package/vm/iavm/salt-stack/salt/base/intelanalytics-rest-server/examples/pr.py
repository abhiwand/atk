from intelanalytics import *

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/movie_data_random.csv"

#csv schema definition
schema = [("user_id", int32),
          ("movie_id", int32),
          ("rating", int32),
          ("splits", str)]

csv_file = CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame"

frame = BigFrame(csv_file)

print "Done building frame"

print "Inspecting frame"

print frame.inspect()

user = VertexRule("user_id", frame.user_id, {"vertex_type": "L"})

movie = VertexRule("movie_id", frame.movie_id, {"vertex_type": "R"})

rates = EdgeRule("edge", user, movie, {"splits": frame.splits, "rating": frame.rating})

print "Creating graph 'pr'"
graph = BigGraph([user, movie, rates], "pr")

print "Running page rank on graph 'pr' "
print graph.ml.page_rank(input_edge_label_list=["edge"], output_vertex_property_list=["pr_result"])

