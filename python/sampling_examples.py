# from intelanalytics import *
#
# # read input file and create dataframe
# csv_file = CsvFile('datasets/test.csv', [('colA', int32), ('colB', int32), ('colC', str)])
# frame = BigFrame(csv_file)
#
# print(frame.inspect())
#
# # vertex rules
# src_vertex_rule = VertexRule('userId', frame['colA'])  # ignore any properties for now
# dest_vertex_rule = VertexRule('userId', frame['colB'])
#
# # edge rule
# edge_rule = EdgeRule('knows', src_vertex_rule, dest_vertex_rule)
#
# # create the graph object
# graph = BigGraph([src_vertex_rule, dest_vertex_rule, edge_rule])
#
# # sample graph
# subgraph = graph.sampling.vertex_sample(5, 'uniform', 1)
# print(subgraph.name)



from intelanalytics import *

csv_file = CsvFile('datasets/netflix/netflix_2million.csv', [('userId', str),
                                                             ('vertexType', str),
                                                             ('movieId', str),
                                                             ('rating', str),
                                                             ('splits', str)])
frame = BigFrame(csv_file)

print(frame.inspect())

user_vertex_rule = VertexRule('userId', frame['userId'])
movie_vertex_rule = VertexRule('movieId', frame['movieId'])

edge_rule = EdgeRule('rating', user_vertex_rule, movie_vertex_rule)

graph = BigGraph([user_vertex_rule, movie_vertex_rule, edge_rule])

subgraph = graph.sampling.vertex_sample(50, 'uniform', 1)
print(subgraph.name)

subgraph = graph.sampling.vertex_sample(50, 'degree', 1)
print(subgraph.name)

subgraph = graph.sampling.vertex_sample(50, 'degreedist', 1)
print(subgraph.name)
