from intelanalytics import *

# read input file and create dataframe
csv_file = CsvFile('datasets/test.csv', [('colA', int32), ('colB', int32), ('colC', str)])
frame = BigFrame(csv_file)

print(frame.inspect())

# vertex rules
src_vertex_rule = VertexRule('userId', frame['colA'])  # ignore any properties for now
dest_vertex_rule = VertexRule('userId', frame['colB'])

# edge rule
edge_rule = EdgeRule('knows', src_vertex_rule, dest_vertex_rule)

# create the graph object
graph = BigGraph([src_vertex_rule, dest_vertex_rule, edge_rule])

# sample graph
subgraph = graph.vertex_sample(size=5)
print(subgraph.name)
