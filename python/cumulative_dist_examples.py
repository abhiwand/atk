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
frame = BigFrame(csv_file)
print(frame.inspect())

cumulative_sum_frame = frame.cumulative_sum('weight')
print(cumulative_sum_frame.inspect())
