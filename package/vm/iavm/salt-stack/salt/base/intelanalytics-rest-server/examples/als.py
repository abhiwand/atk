#!/usr/bin/python2.7
import intelanalytics as ia

ia.connect();

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/movie_data_random.csv"

#csv schema definition
schema = [("user_id", int32),
          ("movie_id", int32),
          ("rating", int32),
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

print "Creating Graph als"

g = ia.TitanGraph([user, movie, rates], "als")

print "Running Alternating Least Squares on Graph als"

print g.ml.alternating_least_squares(edge_value_property_list=["rating"],
                                     vertex_type_property_key="vertex_type",
                                     input_edge_label_list=["edge"],
                                     output_vertex_property_list=["als_result"],
                                     edge_type_property_key="splits",
                                     vector_value=True,
                                     als_lambda=0.065)
