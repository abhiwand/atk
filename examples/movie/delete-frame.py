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

#
# Movie example - create a frame and delete it
#
# Depends on a movie.csv file (see sample in data/movie.csv)
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal movie.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/delete-frame.py')
#

from intelanalytics import *

#loggers.set_http()

print("server ping")
server.ping()

print("define csv file")
csv = CsvFile("/movie.csv", schema= [('user', int32),
                                              ('vertexType', str),
                                              ('movie', int32),
                                              ('rating', str),
                                              ('splits', str)], skip_header_lines=1)

print("create big frames")
frame1 = BigFrame(csv)
frame2 = BigFrame(csv, "myframe")

print("inspect frame 1")
print frame1.inspect(10)

print("delete frame1 passing BigFrame")
print drop_frames(frame1)

print("delete frame2 passing the name")
print drop_frames("myframe")
