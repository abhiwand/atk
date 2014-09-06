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

print "Filter frame by rating"

frame.filter(lambda row: row.rating >= 5)

print frame.inspect()

# Create some rules

user = VertexRule("user_id", frame["user_id"], {"vertex_type": "L"})

movie = VertexRule("movie_id", frame.movie_id)

rates = EdgeRule("rating", user, movie, {"splits": frame.splits}, is_directed=True)

# Create a graph
print "creating graph"
graph = BigGraph([user, movie, rates])


