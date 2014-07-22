from intelanalytics import *

#loggers.set('DEBUG', 'intelanalytics.rest.connection','/home/awwicker/logOutput','True')

input_fileA = "datasets/ecdfDataA.csv"
input_fileB = "datasets/ecdfDataB.csv"

csv_fileA = CsvFile(input_fileA, [('A', int32)])
csv_fileB = CsvFile(input_fileB, [('B', int32)])

frameA = BigFrame(csv_fileA)
frameB = BigFrame(csv_fileB)

print(frameA.inspect())
print(frameB.inspect())

ecdf_frameA = frameA.ecdf('A')
ecdf_frameB = frameB.ecdf('B')

print(ecdf_frameA.inspect())
print(ecdf_frameB.inspect())
