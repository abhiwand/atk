

from intelanalytics import *

#loggers.set_http()

print("server ping")
server.ping()

print("define csv file")
schema =  [("number", int32), ("factor", str), ("binary", str), ("isPrime", str), ("reverse", str), ("isPalindrome", str)]
csv = CsvFile("/numbers.csv", schema, delimiter=":", skip_header_lines=1)

print("create big frame")
frame = BigFrame(csv)

print("inspect frame")
print frame.inspect(30)
print("frame row count " + str(frame.row_count))

print("bin column")
binned = frame.bin_column('number', 4, 'equalwidth', 'number_binned')
print("inspect frame")
print binned.inspect(30)