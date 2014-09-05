from intelanalytics import *

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/movie_data_random.csv"

#csv schema definition
schema = [("user_id", int64),
          ("movie_id", int64),
          ("rating", int64),
          ("splits", str)]

csv_file = CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame"

frame = BigFrame(csv_file)

print "Done building data frame"

print frame.inspect()

user = VertexRule("user_id", frame.user_id, {"vertex_type": "L"})

movie = VertexRule("movie_id", frame.movie_id, {"vertex_type": "R"})

rates = EdgeRule("edge", user, movie, {"splits": frame.splits, "rating": frame.rating})

print "Creating Graph connected_components_demo"

graph = BigGraph([user, movie, rates], "connected_components_demo")

print "Running Connected Components Algorithm"

print graph.ml.connected_components(input_edge_label="edge",
                                    output_vertex_property="component_id")
