from intelanalytics import *

#loggers.set('DEBUG', 'intelanalytics.rest.connection','/home/hadoop/logOutput','True')

binary_input = "datasets/binarymodel.csv"
binary_char_input = "datasets/binarymodelchar.csv"
multiclass_input = "datasets/multimodel.csv"
multiclass_char_input = "datasets/multimodelchar.csv"

binary_csv = CsvFile(binary_input, [('labeled', str), ('predicted', str)])
binary_char_csv = CsvFile(binary_char_input, [('labeled', str), ('predicted', str)])
multiclass_csv = CsvFile(multiclass_input, [('labeled', str), ('predicted', str)])
multiclass_char_csv = CsvFile(multiclass_char_input, [('labeled', str), ('predicted', str)])

binary_frame = BigFrame(binary_csv)
binary_char_frame = BigFrame(binary_char_csv)
multiclass_frame = BigFrame(multiclass_csv)
multiclass_char_frame = BigFrame(multiclass_char_csv)

print(binary_frame.inspect())
print(binary_char_frame.inspect())
print(multiclass_frame.inspect())
print(multiclass_char_frame.inspect())

print(binary_frame.confusion_matrix('labeled', 'predicted'))
