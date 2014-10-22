#!/usr/bin/python2.7
from intelanalytics import *

#the default home directory is  hdfs://user/iauser all the sample data sets are saved to hdfs://user/iauser/datasets
dataset = r"datasets/test_lda.csv"

#csv schema definition
schema = [("doc", str),
          ("vertex_type", str),
          ("word", str),
          ("word_count", int64)]

csv_file = CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame"

frame = Frame(csv_file)

print "Done building data frame"

print "Inspecting frame"

print frame.inspect()

doc = VertexRule("doc", frame.doc, {"vertex_type": "L"})

word = VertexRule("word", frame.word, {"vertex_type": "R"})

contains = EdgeRule("contains", doc, word, {"word_count": frame.word_count}, bidirectional=True)

print "Create graph 'lda'"
graph = TitanGraph([doc, word, contains], "lda")

print "Running Latent Dirichlet Allocation on graph 'lda' "
print graph.ml.latent_dirichlet_allocation(edge_value_property_list = ["word_count"],
                                           vertex_type = "vertex_type",
                                           input_edge_label_list = ["contains"],
                                           output_vertex_property_list = ["lda_result"],
                                           vector_value = True,
                                           max_supersteps = 1,
                                           num_topics = 3)
