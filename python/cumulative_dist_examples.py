from intelanalytics import *

#loggers.set('DEBUG', 'intelanalytics.rest.connection','/home/hadoop/logOutput','True')

#input_file = "datasets/netflix/netflix_2million.csv"
input_file = "datasets/netflix/movie_data_1mb_noheader.csv"
#input_file = "datasets/testdata.csv"
#csv_file = CsvFile(input_file, [('src', str), ('weight', int32)])
csv_file = CsvFile(input_file, [('src', int64),
                                ('vertex_type', str),
                                ('dest', int64),
                                ('weight', int32),
                                ('edge_type', str)])
# input_file = "datasets/cumulativeTest.csv"
# csv_file = CsvFile(input_file, [('a', int64),
#                                 ('b', str),
#                                 ('c', int64),
#                                 ('d', int64)])
frame = BigFrame(csv_file)
print(frame.inspect())

cumulative_sum_frame = frame.cumulative_sum('weight')
print(cumulative_sum_frame.inspect())

cumulative_count_frame = frame.cumulative_count('weight', 1)
print(cumulative_count_frame.inspect())

cumulative_percent_sum_frame = frame.cumulative_percent_sum('weight')
print(cumulative_percent_sum_frame.inspect())

cumulative_percent_count_frame = frame.cumulative_percent_count('weight', 1)
print(cumulative_percent_count_frame.inspect())