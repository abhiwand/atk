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
# column to be used to join on, e.g., the movie recommendation data
#
from intel_analytics.table.bigdataframe import get_frame_builder, get_frame_names, get_frame

fb = get_frame_builder()

# retrieve the preexisting BigDataFrames, if not there, use FrameBuilder.build_from_... () APIs to contruct them from the raw data
#
fL = get_frame('frameA')
fR1 = get_frame('frameB')
fR2 = get_frame('frameC')

# dump
fL.inspect()
fR1.inspect()
fR2.inspect()

i = 0

# single
for how in ['outer', 'inner', 'left', 'right']:
    name = 'fJ%d_single_%s' % (i, how)
    print 'Single right join test: output %s'%name
    fJ = fb.join_data_frame(fL, fR1, how=how, left_on='movie', right_on='movie', suffixes=['_l', '_r'], join_frame_name=name)
    assert fJ is not None
    i=i+1

# self
for how in ['outer', 'inner', 'left', 'right']:
    name = 'fJ%d_self_%s' % (i, how)
    print 'Multiple right join test: output %s'%name
    fJ = fb.join_data_frame(fL, fL, how=how, left_on='movie', right_on='movie', suffixes=['_l', '_r'], join_frame_name=name)
    assert fJ is not None
    i=i+1

# multiple: only supports 2 tables a time for outer
fJ = fb.join_data_frame(fL, [fR1, fR2], \
                        how='inner', \
                        left_on='movie', \
                        right_on=['movie', 'movie'], \
                        suffixes=['_l', '_r1', '_r2'], \
                        join_frame_name=name)
for how in ['outer', 'left', 'right']:
    name = 'fJ%d_multiple_%s' % (i, how)
    print 'Multiple right join test: output %s'%name
    fJ = fb.join_data_frame(fL, fR1, \
                            how=how, \
                            left_on='movie', \
                            right_on='movie', \
                            suffixes=['_l', '_r'], \
                            join_frame_name=name)
    assert fJ is not None
    i=i+1
