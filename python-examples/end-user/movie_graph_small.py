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

print "Done building frame"

print "Inspecting frame"

print frame.inspect()

print "Filter frame by rating"

frame.filter(lambda row: row.rating >= 5)

print frame.inspect()

# Create some rules

user = ia.VertexRule("user_id", frame["user_id"], {"vertex_type": "L"})

movie = ia.VertexRule("movie_id", frame.movie_id)

rates = ia.EdgeRule("rating", user, movie, {"splits": frame.splits}, bidirectional=False)

# Create a graph
print "creating graph"
graph = ia.TitanGraph([user, movie, rates])