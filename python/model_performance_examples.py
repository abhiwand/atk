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

binary_acc = binary_frame.accuracy('labeled', 'predicted')
binary_prec = binary_frame.precision('labeled', 'predicted')
binary_rec = binary_frame.recall('labeled', 'predicted')
binary_f = binary_frame.fmeasure('labeled', 'predicted')
print('binary: {0}, {1}, {2}, {3}'.format(binary_acc, binary_prec, binary_rec, binary_f))

binary_char_acc = binary_char_frame.accuracy('labeled', 'predicted')
binary_char_prec = binary_char_frame.precision('labeled', 'predicted', 'yes')
binary_char_rec = binary_char_frame.recall('labeled', 'predicted', 'yes')
binary_char_f = binary_char_frame.fmeasure('labeled', 'predicted', 'yes')
print('binary_char: {0}, {1}, {2}, {3}'.format(binary_char_acc, binary_char_prec, binary_char_rec, binary_char_f))

multiclass_acc = multiclass_frame.accuracy('labeled', 'predicted')
multiclass_prec = multiclass_frame.precision('labeled', 'predicted')
multiclass_rec = multiclass_frame.recall('labeled', 'predicted')
multiclass_f = multiclass_frame.fmeasure('labeled', 'predicted')
print('multiclass: {0}, {1}, {2}, {3}'.format(multiclass_acc, multiclass_prec, multiclass_rec, multiclass_f))

multiclass_char_acc = multiclass_char_frame.accuracy('labeled', 'predicted')
multiclass_char_prec = multiclass_char_frame.precision('labeled', 'predicted')
multiclass_char_rec = multiclass_char_frame.recall('labeled', 'predicted')
multiclass_char_f = multiclass_char_frame.fmeasure('labeled', 'predicted')
print('multiclass_char: {0}, {1}, {2}, {3}'.format(multiclass_char_acc, multiclass_char_prec, multiclass_char_rec, multiclass_char_f))
