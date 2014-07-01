from intelanalytics import *

#loggers.set('DEBUG', 'intelanalytics.rest.connection','/home/hadoop/logOutput','True')

binary_model_input = "datasets/binarymodel.csv"
multiclass_model_input = "datasets/multimodel.csv"

binary_csv_file = CsvFile(binary_model_input, [('labeled', str), ('predicted', str)])
multiclass_csv_file = CsvFile(multiclass_model_input, [('labeled', str), ('predicted', str)])

binary_frame = BigFrame(binary_csv_file)
multiclass_frame = BigFrame(multiclass_csv_file)

print(binary_frame.inspect())
print(multiclass_frame.inspect())

binary_acc = binary_frame.accuracy('labeled', 'predicted')
print('binary accuracy = {0}'.format(binary_acc))

multiclass_acc = multiclass_frame.accuracy('labeled', 'predicted')
print('multiclass accuracy = {0}'.format(multiclass_acc))
