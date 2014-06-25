from intelanalytics import *

#loggers.set('DEBUG', 'intelanalytics.rest.connection','/home/hadoop/logOutput','True')

#input_file = "datasets/netflix/netflix_2million.csv"
input_file = "datasets/netflix/movie_data_1mb_noheader.csv"
csv_file = CsvFile(input_file, [('src', int64),
                                ('vertex_type', str),
                                ('dest', int64),
                                ('weight', int32),
                                ('edge_type', str)])
frame = BigFrame(csv_file)
print(frame.inspect())

width_frame = frame.bin_column('weight', 3, bin_type='equalwidth', bin_column_name='EWBinned')
print(width_frame.inspect())

width_frame2 = frame.bin_column('weight', 5, bin_type='equalwidth', bin_column_name='EWBinned')
print(width_frame2.inspect())

width_frame3 = frame.bin_column('weight', 6, bin_type='equalwidth', bin_column_name='EWBinned')
print(width_frame3.inspect())

depth_frame = frame.bin_column('weight', 5, bin_type='equaldepth', bin_column_name='EDBinned')
print(depth_frame.inspect())

depth_frame2 = frame.bin_column('weight', 3, bin_type='equaldepth', bin_column_name='EDBinned')
print(depth_frame2.inspect())

# note that identical values are not split across bins
depth_frame3 = frame.bin_column('weight', 2, bin_type='equaldepth', bin_column_name='EDBinned')
print(depth_frame3.inspect())

# should fail in bin_column: unable to bin non-numeric values
bad_frame = frame.bin_column('vertex_type', 2, bin_type='equalwidth', bin_column_name='EWBinned')
print(bad_frame.inspect())
