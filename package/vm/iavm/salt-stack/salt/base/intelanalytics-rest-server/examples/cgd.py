#!/usr/bin/python2.7
import intelanalytics as ia

ia.connect();

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/movie_data_random.csv"

#csv schema definition
schema = [("user_id", ia.int32),
          ("movie_id", ia.int32),
          ("rating", ia.int32),
          ("splits", str)]

csv_file = ia.CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame"

frame = ia.Frame(csv_file)

print "Done building data frame"

print "Inspecting frame"

print frame.inspect()

user = ia.VertexRule("user_id", frame.user_id, {"vertex_type": "L"})

movie = ia.VertexRule("movie_id", frame.movie_id, {"vertex_type": "R"})

rates = ia.EdgeRule("edge", user, movie, {"splits": frame.splits, "rating": frame.rating}, bidirectional=True)

print "Creating Graph cgd"

graph = ia.TitanGraph([user, movie, rates], "cgd")

print "Running Conjugate Gradient Descent on Graph cgd"

print graph.ml.conjugate_gradient_descent(edge_value_property_list=["rating"],
                                          vertex_type_property_key="vertex_type",
                                          input_edge_label_list=["edge"],
                                          output_vertex_property_list=["cgd_result"],
                                          edge_type_property_key="splits",
                                          vector_value=True,
                                          cgd_lambda=0.065,
                                          num_iters=3)
