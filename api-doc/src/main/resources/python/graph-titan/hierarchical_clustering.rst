Build hierarchical clustering over an inital titan graph.


Parameters
----------
none

Returns
-------
a set of titan vertices and edges representing the internal clustering of the graph.

Notes
-----
The internal vertices and edges are for graph navigation and are added to the intial graph.

Examples
--------
sample_graph.txt is a file in the following format: src, dest, distance

1, 2, 1.5f
2, 1, 1.5f
2, 3, 1.5f
3, 2, 1.5f
1, 3, 1.5f
3, 1, 1.5f

the script:

import intelanalytics as ia
ia.connect()
d = "sample_graph.txt"
s = [("src", str), ("dest", str), ("dist", ia.float32)]
c = ia.CsvFile(d,s)
frame = ia.Frame(c, "sample")
print frame.inspect(frame.row_count)
src = ia.VertexRule("vertex", frame.src)
dest = ia.VertexRule("vertex", frame.dest)
dist = ia.EdgeRule("edge", src, dest, {"dist":frame.dist}, bidirectional=True)
print "Creating graph 'sample_graph'"
graph = ia.TitanGraph([src, dest, dist], "sample_graph")
#graph = ia.get_graph("7_9")
graph.hierarchical_clustering()



