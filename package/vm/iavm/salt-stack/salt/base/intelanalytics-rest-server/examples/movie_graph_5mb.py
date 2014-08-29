from intelanalytics import *

dataset = "datasets/movie_sample_data_5mb.csv"

#csv schema definition
schema = [("user_id", int32),
          ("vertex_type", str),
          ("movie_id", int32),
          ("rating", int32),
          ("splits", str)]

print "Defining the params"
csv_file = CsvFile(dataset, schema, skip_header_lines=1)

print "building frame"
frame = BigFrame(csv_file)
print "Done building frame!"

print frame.inspect()

print "filter frame by rating"
frame.filter(lambda row: row.rating >= 5)

print frame.inspect()

# Create some rules

user = VertexRule("user_id", frame["user_id"], {"vertex_type": frame["vertex_type"]})

movie = VertexRule("movie_id", frame.movie_id)

rates = EdgeRule("rating", user, movie, {"splits": frame.splits}, is_directed=True)

# Create a graph
print "creating graph"
graph = BigGraph([user, movie, rates])


