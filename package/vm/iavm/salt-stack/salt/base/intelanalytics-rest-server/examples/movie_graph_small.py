from intelanalytics import *

dataset = "datasets/movie_data_random.csv"

#csv schema definition
schema = [("user_id", int32),
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

user = VertexRule("user_id", frame.user_id, {"vertex_type": "L"})

movie = VertexRule("movie_id", frame.movie_id, {"vertex_type": "R"})

rates = EdgeRule("edge", user, movie, {"splits": frame.splits, "rating": frame.rating})

# Create a graph
print "creating graph"
graph = BigGraph([user, movie, rates])


