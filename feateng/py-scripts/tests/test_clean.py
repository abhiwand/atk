import subprocess
import sys
import shaw_parser
import getopt

"""validation script only for the shaw dataset and for duration field
can be modified other datasets and fields similarly
"""
try:
  opts, args = getopt.getopt(sys.argv[1:], 'ar:')
except getopt.GetoptError:
  sys.exit(1)

validate_average=False
validate_replacement=False
for opt, arg in opts:
  if opt in ('-a'):
    validate_average=True
    print "will validate average values" 
  elif opt in ('-r'):
    validate_replacement=True
    replacement_value=float(arg)
    print "will validate replacement value: ", replacement_value

cat = subprocess.Popen(["hadoop", "dfs", "-cat", "/tmp/sample_shaw.log"], stdout=subprocess.PIPE)
nInRecords = 0
null_durations = 0
duration_total=0
null_duration_lines=[]
for line in cat.stdout:
    parsed = shaw_parser.parseRawRecord(line)
    duration = parsed[3]
    nInRecords+=1
    
    if duration == None:
        null_durations+=1
        null_duration_lines.append(parsed[0])#store the timestamp
#        print parsed
    else:
      duration_total+=float(duration)
      parsed_str = []
      for x in parsed:
          if x == None:
              parsed_str.append('')
          else:
              parsed_str.append(str(x))

duration_avg = duration_total / (nInRecords - null_durations)

print "nInRecords ", nInRecords
print "duration_total ", duration_total
if validate_average:
  print "duration avg", duration_avg
nOutRecords=0
cat = subprocess.Popen(["hadoop", "dfs", "-cat", "/tmp/shaw_processed_csv/part-m-00000"], stdout=subprocess.PIPE)
i=0
print "Validating"
for line in cat.stdout:
    cols = line.split(',')
    assert cols[3] != None#duration column is 3
    duration = float(cols[3])
    if validate_average:
        if cols[0] in null_duration_lines:#check the timestamp to see whether this event has a null duration
#          print "%f vs. %f " % (duration, duration_avg)
          assert (duration - duration_avg) < 0.01 
    if validate_replacement:
      if cols[0] in null_duration_lines:#check the timestamp to see whether this event has a null duration
#        print "%f vs. %f" % (duration, replacement_value)
        assert (duration - replacement_value) < 0.01
    i+=1
    nOutRecords+=1

cat = subprocess.Popen(["hadoop", "dfs", "-cat", "/tmp/shaw_processed_csv/part-m-00001"], stdout=subprocess.PIPE)
for line in cat.stdout:
    cols = line.split(',')
    assert cols[3] != None#duration column is 3
    duration = float(cols[3])
    if validate_average:
        if cols[0] in null_duration_lines:#check the timestamp to see whether this event has a null duration
 #         print "%f vs. %f " % (duration, duration_avg)
          assert (duration - duration_avg) < 0.01 
    if validate_replacement:
      if cols[0] in null_duration_lines:#check the timestamp to see whether this event has a null duration
        assert (duration - replacement_value) < 0.01
    i+=1
    nOutRecords+=1
print "Done"
if validate_average or validate_replacement:
  expected_output_recs = nInRecords
else:
  expected_output_recs = (nInRecords - null_durations)
print 'Expected output records: ', expected_output_recs
print 'Actual output records: ', nOutRecords
assert nOutRecords == expected_output_recs
print "Validation done"
        
