import os
import sys
import subprocess
import commands
import math
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base_script_path + '/..')
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.config import CONFIG_PARAMS

print "Using", CONFIG_PARAMS
print 'Starting ...'

#first set up the environment variables
os.environ["PATH"] = '/home/user/pig-0.12.0/bin' + ':' + os.environ["PATH"]
os.environ["JYTHONPATH"]  = os.getcwd() + '/py-scripts/' # need for shipping the scripts that we depend on to worker nodes

print ">> JYTHONPATH",os.environ["JYTHONPATH"]

TEST_TABLE='us_states'
SHOULD_IMPORT=False

SUBSTR_LENGTH=3
TEMP_TABLES=['endswith','equalsIgnoreCase', 'indexof', 'lastindexof',
             'lower', 'ltrim', 'regex', 'extract', 'regex_extract_all',
             'replace', 'rtrim', 'startswith', 'strsplit', 'substring',
             'trim', 'upper', 'tokenize','length']

print 'Cleaning up all the temp tables & their schema definitions'
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    for temp in TEMP_TABLES:
        try:
            hbase_client.connection.delete_table(temp, disable=True)
            print 'deleted table',temp
        except:
            pass
        try:
            table = hbase_client.connection.table(CONFIG_PARAMS['etl-schema-table'])
            table.delete(temp)#also remove the schema info
        except:
            pass
        
############################################################
# FUNCTIONS TO VALIDATE THE OUTPUT OF THE STRING FUNCTIONS
############################################################
def validate_endswith():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('endswith')
        for key, data in t.scan():
            if data['etl-cf:state_name']== "Connecticut":
                assert data['etl-cf:endswith'] == 'true'
            else:
                assert data['etl-cf:endswith'] == 'false'

def validate_equalsignorecase():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('equalsIgnoreCase')
        for key, data in t.scan():
            if data['etl-cf:state_name']== "Connecticut":
                assert data['etl-cf:equalsIgnoreCase'] == 'true'
            else:
                assert data['etl-cf:equalsIgnoreCase'] == 'false'
                
def validate_indexof():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('indexof')
        for key, data in t.scan():
            if data['etl-cf:state_name']== "Connecticut":
                assert data['etl-cf:indexof'] == '0'
            else:
                assert data['etl-cf:indexof'] == '-1'    

def validate_lastindexof():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('lastindexof')
        for key, data in t.scan():
            if data['etl-cf:state_name']== "Connecticut":
                assert data['etl-cf:lastindexof'] == '1'
            else:
                assert data['etl-cf:lastindexof'] == '-1'      
    
def validate_lower():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('lower')
        for key, data in t.scan():
            assert data['etl-cf:lower'] == data['etl-cf:state_name'].lower()
    
def validate_identical(table_name):
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table(table_name)
        for key, data in t.scan():
            assert data['etl-cf:' + table_name] == data['etl-cf:state_name']
    
def validate_regexp():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('regex_extract')
        for key, data in t.scan():
            if data['etl-cf:state_name'] == 'Connecticut':
                assert data['etl-cf:regex_extract'] == 'Connecticut'
            else:
                assert data['etl-cf:regex_extract'] == ''
        
def validate_regextract_all():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('regex_extract_all')
        for key, data in t.scan():
            if data['etl-cf:state_name'] == 'Connecticut':
                assert data['etl-cf:regex_extract_all'] == '(Connecticut)'
            else:
                assert data['etl-cf:regex_extract_all'] == ''
        
def validate_replace():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('replace')
        for key, data in t.scan():
            if data['etl-cf:state_name'] == 'Connecticut':
                assert data['etl-cf:replace'] == 'Cxxxticut'
            else:
                assert data['etl-cf:replace'] == data['etl-cf:state_name']    

def validate_startswith():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('startswith')
        for key, data in t.scan():
            if data['etl-cf:state_name'] == 'Connecticut':
                assert data['etl-cf:startswith'] == 'true'
            else:
                assert data['etl-cf:startswith'] == 'false'

def validate_strsplit():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('strsplit')
        for key, data in t.scan():
            state = data['etl-cf:state_name']
            result = data['etl-cf:strsplit']
            if state == 'New Jersey':
                assert result == '(New,Jersey)'
            elif state == 'New Hampshire':
                assert result == '(New,Hampshire)'        
            elif state == 'New Mexico':
                assert result == '(New,Mexico)' 
            elif state == 'New York':
                assert result == '(New,York)' 
            elif state == 'North Carolina':
                assert result == '(North,Carolina)' 
            elif state == 'North Dakota':
                assert result == '(North,Dakota)'   
            elif state == 'Rhode Island':
                assert result == '(Rhode,Island)' 
            elif state == 'South Carolina':
                assert result == '(South,Carolina)'         
            elif state == 'South Dakota':
                assert result == '(South,Dakota)'   
            elif state == 'West Virginia':
                assert result == '(West,Virginia)'           
            else:
                assert result == '('+state+')'                

def validate_substring():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('substring')
        for key, data in t.scan():
            state = data['etl-cf:state_name']
            result = data['etl-cf:substring']
            assert result == state[:SUBSTR_LENGTH]
    
def validate_upper():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('upper')
        for key, data in t.scan():
            assert data['etl-cf:upper'] == data['etl-cf:state_name'].upper()
    
#TOKENIZE returns bag!
def validate_tokenize():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('tokenize')
        for key, data in t.scan():
            state = data['etl-cf:state_name']
            result = data['etl-cf:tokenize']
            #     print "-%s-vs.-%s-" % (state,result)
            if state == 'New Jersey':
                assert result == '{(New),(Jersey)}'
            elif state == 'New Hampshire':
                assert result == '{(New),(Hampshire)}'        
            elif state == 'New Mexico':
                assert result == '{(New),(Mexico)}' 
            elif state == 'New York':
                assert result == '{(New),(York)}' 
            elif state == 'North Carolina':
                assert result == '{(North),(Carolina)}' 
            elif state == 'North Dakota':
                assert result == '{(North),(Dakota)}'   
            elif state == 'Rhode Island':
                assert result == '{(Rhode),(Island)}' 
            elif state == 'South Carolina':
                assert result == '{(South),(Carolina)}'         
            elif state == 'South Dakota':
                assert result == '{(South),(Dakota)}'   
            elif state == 'West Virginia':
                assert result == '{(West),(Virginia)}'           
            else:
                assert result == '{('+state+')}'    
        
def validate_length():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('length')
        for key, data in t.scan():
            assert int(data['etl-cf:length']) == len(data['etl-cf:state_name'])    
#############################################################################
    
if SHOULD_IMPORT:
    #cleanup test tables
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        hbase_client.drop_create_table(TEST_TABLE, [CONFIG_PARAMS['etl-column-family']])
                 
    print "------------------------------------TESTING IMPORT SCRIPTS------------------------------------"
    commands.getoutput('hadoop fs -rmr /tmp/us_states.csv')
    commands.getoutput('hadoop fs -put test-data/us_states.csv /tmp/us_states.csv')
    print "Uploaded /tmp/us_states.csv to HDFS:/tmp/us_states.csv"
      
    subprocess.call(['python', 'py-scripts/import_csv.py', '-i', '/tmp/us_states.csv',
                     '-o', TEST_TABLE, '-s', 'state_name:chararray', '-k'])
          
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        data_dict = hbase_client.get(TEST_TABLE,'1')#get the first row
        print "got", data_dict
        assert data_dict[CONFIG_PARAMS['etl-column-family']+'state_name'] == 'Alabama'
  
            
args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'endswith', '-f', 'state_name', '-n', 'endswith', '-t', 'ENDSWITH', '-a', '["icut"]', '-k']
subprocess.call(args)
validate_endswith()
print 'Validated ENDSWITH'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'equalsIgnoreCase', '-f', 'state_name', '-n', 'equalsIgnoreCase', '-t', 'EqualsIgnoreCase', '-a', '["connecticut"]', '-k']
subprocess.call(args)
validate_equalsignorecase()
print 'Validated EqualsIgnoreCase'
 
args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'indexof', '-f', 'state_name', '-n', 'indexof', '-t', 'INDEXOF', '-a', '["Connecticut",0]', '-k']
subprocess.call(args)
validate_indexof()
print 'Validated INDEXOF'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'lastindexof', '-f', 'state_name', '-n', 'lastindexof', '-t', 'LAST_INDEX_OF', '-a', '["onnec"]', '-k']
subprocess.call(args)
validate_lastindexof()
print 'Validated LAST_INDEX_OF'
 
args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'lower', '-f', 'state_name', '-n', 'lower', '-t', 'LOWER', '-k']
subprocess.call(args)
validate_lower()
print 'Validated LOWER'
 
args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'ltrim', '-f', 'state_name', '-n', 'ltrim', '-t', 'LTRIM', '-k']
subprocess.call(args)
validate_identical('ltrim')
print 'Validated LTRIM'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'regex_extract', '-f', 'state_name', '-n', 'regex_extract', '-t', 'REGEX_EXTRACT', '-a', '[r"\\\S*onnec\\\S*",0]', '-k']
subprocess.call(args)
validate_regexp()
print 'Validated REGEX_EXTRACT'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'regex_extract_all', '-f', 'state_name', '-n', 'regex_extract_all', '-t', 'REGEX_EXTRACT_ALL', '-a', '[r"(\\\S*onn\\\S*)"]', '-k']
subprocess.call(args)
validate_regextract_all()
print 'Validated REGEX_EXTRACT_ALL'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'replace', '-f', 'state_name', '-n', 'replace', '-t', 'REPLACE', '-a', '["onnec","xxx"]' , '-k']
subprocess.call(args)
validate_replace()
print 'Validated REPLACE'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'rtrim', '-f', 'state_name', '-n', 'rtrim', '-t', 'RTRIM', '-k']
subprocess.call(args)
validate_identical('rtrim')
print 'Validated RTRIM'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'startswith', '-f', 'state_name', '-n', 'startswith', '-t', 'STARTSWITH', '-a', '["Connec"]' , '-k']
subprocess.call(args)
validate_startswith()
print 'Validated STARTSWITH'
 
args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'strsplit', '-f', 'state_name', '-n', 'strsplit', '-t', 'STRSPLIT', '-a', '[" ",2]' , '-k']
subprocess.call(args)
validate_strsplit()
print 'Validated STRSPLIT'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'substring', '-f', 'state_name', '-n', 'substring', '-t', 'SUBSTRING', '-a', '[0,3]' , '-k']
subprocess.call(args)
validate_substring()
print 'Validated SUBSTRING'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'trim', '-f', 'state_name', '-n', 'trim', '-t', 'TRIM', '-k']
subprocess.call(args)
validate_identical('trim')
print 'Validated TRIM'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'upper', '-f', 'state_name', '-n', 'upper', '-t', 'UPPER', '-k']
subprocess.call(args)
validate_upper()
print 'Validated UPPER'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'tokenize', '-f', 'state_name', '-n', 'tokenize', '-t', 'TOKENIZE', '-k']
subprocess.call(args)
validate_tokenize()
print 'Validated TOKENIZE'

args = ['python', 'py-scripts/transform.py', '-i', 'us_states', '-o', 'length', '-f', 'state_name', '-n', 'length', '-t', 'org.apache.pig.piggybank.evaluation.string.LENGTH', '-k']
subprocess.call(args)
validate_length()
print 'Validated LENGTH'

print 'Cleaning up all the temp tables & their schema definitions'
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    for temp in TEMP_TABLES:
        try:
            hbase_client.connection.delete_table(temp, disable=True)
            print 'deleted table',temp
        except:
            pass
        try:
            table = hbase_client.connection.table(CONFIG_PARAMS['etl-schema-table'])
            table.delete(temp)#also remove the schema info
        except:
            pass
   

# python py-scripts/transform.py -i us_states -o endswith -f state_name  -n endswith -t ENDSWITH -a '["icut"]'
# python py-scripts/transform.py -i us_states -o equalsIgnoreCase -f state_name  -n equalsIgnoreCase -t EqualsIgnoreCase -a '["connecticut"]'
# python py-scripts/transform.py -i us_states -o indexof -f state_name  -n indexof -t INDEXOF -a '["Connecticut",0]'
# python py-scripts/transform.py -i us_states -o lastindexof -f state_name  -n lastindexof -t LAST_INDEX_OF -a '["onnec"]'
# python py-scripts/transform.py -i us_states -o lower -f state_name  -n lower -t LOWER
# python py-scripts/transform.py -i us_states -o ltrim -f state_name  -n ltrim -t LTRIM
# python py-scripts/transform.py -i us_states -o regex_extract -f state_name  -n regex_extract -t REGEX_EXTRACT -a '[r"\\\S*onnec\\\S*",0]'#need to pass as raw string literal, otherwise it is hard to keep the regexp format correct
# python py-scripts/transform.py -i us_states -o regex_extract_all -f state_name  -n regex_extract_all -t REGEX_EXTRACT_ALL -a '[r"(\\\S*onn\\\S*)",0]'
# python py-scripts/transform.py -i us_states -o replace -f state_name  -n replace -t REPLACE -a '["onnec","xxx"]' 
# python py-scripts/transform.py -i us_states -o rtrim -f state_name  -n rtrim -t RTRIM
# python py-scripts/transform.py -i us_states -o startswith -f state_name  -n startswith -t STARTSWITH -a '["Connec"]'
# python py-scripts/transform.py -i us_states -o strsplit -f state_name  -n strsplit -t STRSPLIT -a '[" ",2]'
# python py-scripts/transform.py -i us_states -o substring -f state_name  -n substring -t SUBSTRING -a '[0,3]'
# python py-scripts/transform.py -i us_states -o trim -f state_name  -n trim -t TRIM
# python py-scripts/transform.py -i us_states -o upper -f state_name  -n upper -t UPPER
# python py-scripts/transform.py -i us_states -o tokenize -f state_name  -n tokenize -t TOKENIZE
# python py-scripts/transform.py -i us_states -o length -f state_name  -n length -t org.apache.pig.piggybank.evaluation.string.LENGTH 
    