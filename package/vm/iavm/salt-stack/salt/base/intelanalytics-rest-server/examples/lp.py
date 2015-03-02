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
dataset = r"datasets/lp_edge.csv"

#csv schema definition
schema = [("source", ia.int64),
          ("input_value", str),
          ("target", ia.int64),
          ("weight", ia.float64)]

#csv schema definition
csv = ia.CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame 'lp'"

frame = ia.Frame(csv, "lp")

print "Done building data frame"

print "Inspecting frame 'lp'"

print frame.inspect()

source = ia.VertexRule("source", frame.source, {"input_value" : frame.input_value})

target = ia.VertexRule("target", frame.target, {"input_value" : frame.input_value})

edge = ia.EdgeRule("edge", source, target, {'weight': frame.weight}, bidirectional=True)

print "Creating graph 'lp_graph'"

graph = ia.TitanGraph([source, target, edge], "lp_graph")

print "Running Label Propagation on Graph 'lp_graph'"

print graph.ml.label_propagation(vertex_value_property_list = ["input_value"],
                                 edge_value_property_list = ["weight"],
                                 input_edge_label_list = ["edge"],
                                 output_vertex_property_list = ["lp_posterior"],
                                 vector_value = True,
                                 max_supersteps = 10,
                                 convergence_threshold = 0.0,
                                 anchor_threshold = 0.9,
                                 lp_lambda = 0.5,
                                )
