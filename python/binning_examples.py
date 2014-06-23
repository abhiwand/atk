from intelanalytics import *

#loggers.set_http()

csv_file = CsvFile("datasets/test.csv", [('colA', int32), ('colB', int32), ('colC', str)])
frame = BigFrame(csv_file)
print(frame.inspect())

width_frame = frame.bin_column('colB', 5, bin_type='equalwidth', bin_column_name='EWBinned')
print(width_frame.inspect())

width_frame2 = frame.bin_column('colB', 3, bin_type='equalwidth', bin_column_name='EWBinned')
print(width_frame2.inspect())

width_frame3 = frame.bin_column('colA', 2, bin_type='equalwidth', bin_column_name='EWBinned')
print(width_frame3.inspect())

depth_frame = frame.bin_column('colB', 5, bin_type='equaldepth', bin_column_name='EDBinned')
print(depth_frame.inspect())

depth_frame2 = frame.bin_column('colB', 3, bin_type='equaldepth', bin_column_name='EDBinned')
print(depth_frame2.inspect())

# note that identical values are not split across bins
depth_frame3 = frame.bin_column('colA', 2, bin_type='equaldepth', bin_column_name='EDBinned')
print(depth_frame3.inspect())

# should fail in bin_column: unable to bin non-numeric values
#bad_frame = frame.bin_column('colC', 2, bin_type='equalwidth', bin_column_name='EWBinned')
#print(bad_frame.inspect())
