##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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
##############################################################################i
#
# Notes:
#
# This script is expected to be run in a established IntelAnalytics environment,
#
# This script is pretty dumb and expects 3 prexisting BigDataFrames as frameA,
# frameB, frameC, you can use whatever raw data to build these three but the 
# schema must have a column name 'movie', which is used in this test as the 
# column to be used to join on.
#
# The example raw data is pointing to a small set of medline data

from intel_analytics.table.bigdataframe import get_frame_builder, get_frame_names, get_frame
import time
start_time = time.time()
fb = get_frame_builder()
sourceFile1='/user/hadoop/medline/medline-terms-toy-1000rows-1.csv'
sourceFile2='/user/hadoop/medline/medline-terms-toy-1000rows-2.csv'
sourceFile3='/user/hadoop/medline/mesh-ontology-5-levels.csv'

medTermSchema='article_id: chararray, year: int, term: chararray, qualifiers: chararray'
medOntoSchema='treenumber: chararray, type: chararray, term: chararray'


#
def load_and_build(name, filename, schema):
    try:
        mL = get_frame(name)
        print 'Loaded %s originally imported from %s' % (name, filename)
    except:
        print 'Frame %s not found, loading from %s...' % (name, filename)
        mL = fb.build_from_csv(frame_name=name,     \
                               file_name=filename,  \
                               schema=schema,       \
                               overwrite=True)
        print 'Imported %s imported from %s' % (name, filename)
    return mL


# load frame
frame1 = load_and_build('sourceFrame1', sourceFile1, medTermSchema)
frame2 = load_and_build('sourceFrame2', sourceFile1, medTermSchema)
frame3 = load_and_build('sourceFrame3', sourceFile3, medOntoSchema)

frame1.inspect()
frame2.inspect()
frame3.inspect()

# for medline data, we do inner
how='inner'
print 'Join %s and %s' % (sourceFile1, sourceFile3)
mJ1 = fb.join_data_frame(frame1, frame3,        \
                         how=how,    \
                         left_on='term', \
                         right_on='term',\
                         suffixes=['_l', '_r'], \
                         join_frame_name='medJoin1')
mJ1.inspect()

# outer
how='outer'
print 'Join(%s): %s, %s and %s' % (how, sourceFile2, sourceFile3)
mJ2 = fb.join_data_frame(frame2, frame3, \
                         how=how, \
                         left_on='term', \
                         right_on='term', \
                         suffixes=['_l', '_r'], \
                         join_frame_name='medJoin2')
mJ2.inspect()

# self
how='outer'
print 'Join(%s): self on %s' % (how, sourceFile1)
mJ3 = fb.join_data_frame(frame1, frame1, \
                         how=how, \
                         left_on='term', \
                         right_on='term', \
                         suffixes=['_1', '_2'], \
                         join_frame_name='medJoin3')
mJ3.inspect()

# multiple tables, only inner
how='inner'
print 'Join(%s): %s, %s and %s' % (how, sourceFile1, sourceFile2, sourceFile3)
mJ4 = fb.join_data_frame(frame1,    \
                         [frame2, frame3],  \
                         how=how, \
                         left_on='term', \
                         right_on=['term', 'term'] \
                         suffixes=['_l', '_r1', '_r2'], \
                         join_frame_name='medJoin4')
mJ4.inspect()
