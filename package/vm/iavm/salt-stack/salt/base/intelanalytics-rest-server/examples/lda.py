from intelanalytics import *

dataset = r"datasets/test_lda.csv"

schema = [("doc", str), ("vertex_type", str), ("word", str), ("word_count", int64)]

csv_file = CsvFile(dataset, schema, skip_header_lines = 0)

f = BigFrame(csv_file)

f.inspect()

doc = VertexRule("doc", f.doc, { "vertex_type": "L"})

word = VertexRule("word", f.word, { "vertex_type": "R"})

contains = EdgeRule("contains", doc, word, { "word_count": f.word_count })

g = BigGraph([doc, word, contains] ,"lda")

g.ml.latent_dirichlet_allocation(edge_value_property_list = "word_count", vertex_type_property_key = "vertex_type", input_edge_label_list = "contains", output_vertex_property_list = "lda_result ", vector_value = "true", num_topics = 3)
