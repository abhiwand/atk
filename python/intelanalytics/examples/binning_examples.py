from intelanalytics import *

#loggers.set_http()

csv_file = CsvFile("datasets/test.csv", [('A', int32), ('B', int32)])
frame = BigFrame(CsvFile(csv_file))
print(frame.inspect())

frame.bin_column('B', 2, bin_type='equalwidth', bin_column_name='BBinned')
print(frame.inspect())
