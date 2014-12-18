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

print "Done building frame"

print "Inspecting frame"

print frame.inspect()

print "Filter frame by rating"

frame.filter(lambda row: row.rating >= 5)

print frame.inspect()

# Create some rules

user = ia.VertexRule("user_id", frame["user_id"], {"vertex_type": "L"})

movie = ia.VertexRule("movie_id", frame.movie_id)

rates = ia.EdgeRule("rating", user, movie, {"splits": frame.splits}, bidirectional=False)

# Create a graph
print "creating graph"
graph = ia.TitanGraph([user, movie, rates])


