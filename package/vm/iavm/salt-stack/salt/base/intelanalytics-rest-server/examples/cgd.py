##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

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

print "Done building data frame"

print "Inspecting frame"

print frame.inspect()

user = ia.VertexRule("user_id", frame.user_id, {"vertex_type": "L"})

movie = ia.VertexRule("movie_id", frame.movie_id, {"vertex_type": "R"})

rates = ia.EdgeRule("edge", user, movie, {"splits": frame.splits, "rating": frame.rating}, bidirectional=True)

print "Creating Graph cgd"

graph = ia.TitanGraph([user, movie, rates], "cgd")

print "Running Conjugate Gradient Descent on Graph cgd"

print graph.ml.conjugate_gradient_descent(edge_value_property_list=["rating"],
                                          vertex_type_property_key="vertex_type",
                                          input_edge_label_list=["edge"],
                                          output_vertex_property_list=["cgd_result"],
                                          edge_type_property_key="splits",
                                          vector_value=True,
                                          cgd_lambda=0.065,
                                          num_iters=3)
