from intelanalytics import *

#loggers.set_http()

csv_file = CsvFile("datasets/test.csv", [('A', int32), ('B', int32)])
frame = BigFrame(csv_file)
print(frame.inspect())

binned_frame = frame.bin_column('B', 5, bin_type='equalwidth', bin_column_name='BBinned')
print(binned_frame.inspect())
