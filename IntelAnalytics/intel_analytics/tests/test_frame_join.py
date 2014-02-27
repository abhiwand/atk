# This script is expected to be run in a established IntelAnalytics environment, i.e.,
#
# > source $INTEL_ANALYTICS_HOME/virtpy/bin/activate
# > ipython console
# > %run $INTEL_ANALYTICS_HOME/intel_analytics/tests/test_frame_join.py
#
#
# Expects 3 prexisting BigDataFrames as frameA, frameB, frameC, you can use whatever 
# raw data to build these three but the schema must have a column name 'movie', which
# is used in this test
#
from intel_analytics.table.bigdataframe import get_frame_builder, get_frame_names, get_frame

fb = get_frame_builder()


fL = get_frame('frameA')
fR1 = get_frame('frameB')
fR2 = get_frame('frameC')

fL.inspect()
fR1.inspect()
fR2.inspect()

i = 0

# single
for how in ['outer', 'inner', 'left', 'right']:
    name = 'fJ%d_single_%s' % (i, how)
    print 'Single right join test: output %s'%name
    fJ = fb.join_data_frame(fL, fR1, how=how, left_on='movie', right_on='movie', suffixes=['_l', '_r'], join_frame_name=name)
    fJ.inspect()
    i=i+1

# self
for how in ['outer', 'inner', 'left', 'right']:
    name = 'fJ%d_self_%s' % (i, how)
    print 'Multiple right join test: output %s'%name
    fJ = fb.join_data_frame(fL, fL, how=how, left_on='movie', right_on='movie', suffixes=['_l', '_r'], join_frame_name=name)
    fJ.inspect()
    i=i+1

# multiple
for how in ['outer', 'inner', 'left', 'right']:
    name = 'fJ%d_multiple_%s' % (i, how)
    print 'Multiple right join test: output %s'%name
    fJ = fb.join_data_frame(fL, [fR1, fR2], \
                            how='outer', \
                            left_on='movie', \
                            right_on=['movie', 'movie'], \
                            suffixes=['_l', '_r1', '_r2'], \
                            join_frame_name=name)
    fJ.inspect()
    i=i+1
