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
dataset = r"datasets/lbp_edge.csv"

#csv schema definition
schema = [("source", ia.int64),
          ("value", str),
          ("vertex_type", str),
          ("target", ia.int64),
          ("weight", ia.float64)]

csv = ia.CsvFile(dataset, schema, skip_header_lines=1)

print "Building data frame 'myframe'"

frame = ia.Frame(csv, "myframe")

print "Done building data frame 'myframe'"

print "Inspecting frame 'myframe'"

print frame.inspect()

source = ia.VertexRule("source", frame.source, {"vertex_type": frame.vertex_type, "value": frame.value})

target = ia.VertexRule("target", frame.target, {"vertex_type": frame.vertex_type, "value": frame.value})

edge = ia.EdgeRule("edge", target, source, {'weight': frame.weight}, bidirectional=True)

print "Creating Graph 'mygraph'"

graph = ia.TitanGraph([target, source, edge], "mygraph")

print "Running Loopy Belief Propagation on Graph mygraph"

print graph.ml.loopy_belief_propagation(vertex_value_property_list=["value"],
                                        edge_value_property_list=["weight"],
                                        input_edge_label_list=["edge"],
                                        output_vertex_property_list=["lbp_posterior"],
                                        vertex_type="vertex_type",
                                        vector_value=True,
                                        max_supersteps=10,
                                        convergence_threshold=0.0,
                                        anchor_threshold=0.9,
                                        smoothing=2.0,
                                        ignore_vertex_type=False,
                                        max_product=False,
                                        power=0)
