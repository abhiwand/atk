import sys
import subprocess
import commands
import math
import os
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..'))
os.environ['PYTHONPATH'] = ':'.join(sys.path)#python scripts that call our pig scripts need this
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.config import global_config as CONFIG_PARAMS

us_states_csv_path = os.path.join(base_script_path, '..', '..', 'feateng', 'test-data/us_states.csv')
py_scripts_path = os.path.join(base_script_path, '..', 'intel_analytics', 'table' , 'hbase', 'scripts')

TEST_TABLE='us_states'
SUBSTR_LENGTH=3
TEMP_TABLES=['endswith','equalsIgnoreCase', 'indexof', 'lastindexof',
             'lower', 'ltrim', 'regex', 'extract', 'regex_extract_all',
             'replace', 'rtrim', 'startswith', 'strsplit', 'substring',
             'trim', 'upper', 'tokenize','length']

print 'Cleaning up all the temp tables & their schema definitions'
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    for temp in TEMP_TABLES:
        try:
            hbase_client.connection.delete_table(temp, disable=True)
            print 'deleted table',temp
        except:
            pass
        try:
            table = hbase_client.connection.table(CONFIG_PARAMS['hbase_schema_table'])
            table.delete(temp)#also remove the schema info
        except:
            pass
        
############################################################
# FUNCTIONS TO VALIDATE THE OUTPUT OF THE STRING FUNCTIONS
############################################################
def validate_endswith():
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('endswith')
        for key, data in t.scan():
            if data['etl-cf:state_name']== "Connecticut":
                assert data['etl-cf:endswith'] == 'true'
            else:
                assert data['etl-cf:endswith'] == 'false'

def validate_equalsignorecase():
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('equalsIgnoreCase')
        for key, data in t.scan():
            if data['etl-cf:state_name']== "Connecticut":
                assert data['etl-cf:equalsIgnoreCase'] == 'true'
            else:
                assert data['etl-cf:equalsIgnoreCase'] == 'false'
                
def validate_indexof():
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('indexof')
        for key, data in t.scan():
            if data['etl-cf:state_name']== "Connecticut":
                assert data['etl-cf:indexof'] == '0'
            else:
                assert data['etl-cf:indexof'] == '-1'    

def validate_lastindexof():
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('lastindexof')
        for key, data in t.scan():
            if data['etl-cf:state_name']== "Connecticut":
                assert data['etl-cf:lastindexof'] == '1'
            else:
                assert data['etl-cf:lastindexof'] == '-1'      
    
def validate_lower():
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('lower')
        for key, data in t.scan():
            assert data['etl-cf:lower'] == data['etl-cf:state_name'].lower()
    
def validate_identical(table_name):
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table(table_name)
        for key, data in t.scan():
            assert data['etl-cf:' + table_name] == data['etl-cf:state_name']
    
def validate_regexp():
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('regex_extract')
        for key, data in t.scan():
            if data['etl-cf:state_name'] == 'Connecticut':
                assert data['etl-cf:regex_extract'] == 'Connecticut'
            else:
                assert data['etl-cf:regex_extract'] == ''
        
def validate_regextract_all():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('regex_extract_all')
        for key, data in t.scan():
            if data['etl-cf:state_name'] == 'Connecticut':
                assert data['etl-cf:regex_extract_all'] == '(Connecticut)'
            else:
                assert data['etl-cf:regex_extract_all'] == ''
        
def validate_replace():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('replace')
        for key, data in t.scan():
            if data['etl-cf:state_name'] == 'Connecticut':
                assert data['etl-cf:replace'] == 'Cxxxticut'
            else:
                assert data['etl-cf:replace'] == data['etl-cf:state_name']    

def validate_startswith():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('startswith')
        for key, data in t.scan():
            if data['etl-cf:state_name'] == 'Connecticut':
                assert data['etl-cf:startswith'] == 'true'
            else:
                assert data['etl-cf:startswith'] == 'false'

def validate_strsplit():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
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
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('substring')
        for key, data in t.scan():
            state = data['etl-cf:state_name']
            result = data['etl-cf:substring']
            assert result == state[:SUBSTR_LENGTH]
    
def validate_upper():
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('upper')
        for key, data in t.scan():
            assert data['etl-cf:upper'] == data['etl-cf:state_name'].upper()
    
#TOKENIZE returns bag!
def validate_tokenize():   
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
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
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table('length')
        for key, data in t.scan():
            assert int(data['etl-cf:length']) == len(data['etl-cf:state_name'])    
#############################################################################
print '###########################'
print 'Validating String Functions'
print '###########################'
#cleanup test table
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    hbase_client.drop_create_table(TEST_TABLE, [CONFIG_PARAMS['hbase_column_family']])
             
print "------------------------------------TESTING IMPORT SCRIPTS------------------------------------"
commands.getoutput('cp %s /tmp/us_states.csv' % (us_states_csv_path))
print "Copied %s to /tmp/us_states.csv" % (us_states_csv_path)
  
return_code = subprocess.call(['python', os.path.join(py_scripts_path, 'import_csv.py'), '-i', '/tmp/us_states.csv',
                 '-o', TEST_TABLE, '-s', 'state_name:chararray', '-k'])

if return_code:
    raise Exception("Import (in string function tests) script failed!")
    sys.exit(1)
    
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    data_dict = hbase_client.get(TEST_TABLE,'1')#get the first row
    print "got", data_dict
    assert data_dict[CONFIG_PARAMS['hbase_column_family']+'state_name'] == 'Alabama'
            
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'endswith', '-f', 'state_name', '-n', 'endswith', '-t', 'ENDSWITH', '-a', '["icut"]', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_endswith()
print 'Validated ENDSWITH'

args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'equalsIgnoreCase', '-f', 'state_name', '-n', 'equalsIgnoreCase', '-t', 'EqualsIgnoreCase', '-a', '["connecticut"]', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_equalsignorecase()
print 'Validated EqualsIgnoreCase'
  
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'indexof', '-f', 'state_name', '-n', 'indexof', '-t', 'INDEXOF', '-a', '["Connecticut",0]', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_indexof()
print 'Validated INDEXOF'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'lastindexof', '-f', 'state_name', '-n', 'lastindexof', '-t', 'LAST_INDEX_OF', '-a', '["onnec"]', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_lastindexof()
print 'Validated LAST_INDEX_OF'
  
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'lower', '-f', 'state_name', '-n', 'lower', '-t', 'LOWER', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_lower()
print 'Validated LOWER'
  
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'ltrim', '-f', 'state_name', '-n', 'ltrim', '-t', 'LTRIM', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_identical('ltrim')
print 'Validated LTRIM'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'regex_extract', '-f', 'state_name', '-n', 'regex_extract', '-t', 'REGEX_EXTRACT', '-a', '[r"\\\S*onnec\\\S*",0]', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_regexp()
print 'Validated REGEX_EXTRACT'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'regex_extract_all', '-f', 'state_name', '-n', 'regex_extract_all', '-t', 'REGEX_EXTRACT_ALL', '-a', '[r"(\\\S*onn\\\S*)"]', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_regextract_all()
print 'Validated REGEX_EXTRACT_ALL'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'replace', '-f', 'state_name', '-n', 'replace', '-t', 'REPLACE', '-a', '["onnec","xxx"]' , '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_replace()
print 'Validated REPLACE'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'rtrim', '-f', 'state_name', '-n', 'rtrim', '-t', 'RTRIM', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_identical('rtrim')
print 'Validated RTRIM'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'startswith', '-f', 'state_name', '-n', 'startswith', '-t', 'STARTSWITH', '-a', '["Connec"]' , '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_startswith()
print 'Validated STARTSWITH'
  
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'strsplit', '-f', 'state_name', '-n', 'strsplit', '-t', 'STRSPLIT', '-a', '[" ",2]' , '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_strsplit()
print 'Validated STRSPLIT'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'substring', '-f', 'state_name', '-n', 'substring', '-t', 'SUBSTRING', '-a', '[0,3]' , '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_substring()
print 'Validated SUBSTRING'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'trim', '-f', 'state_name', '-n', 'trim', '-t', 'TRIM', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_identical('trim')
print 'Validated TRIM'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'upper', '-f', 'state_name', '-n', 'upper', '-t', 'UPPER', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_upper()
print 'Validated UPPER'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'tokenize', '-f', 'state_name', '-n', 'tokenize', '-t', 'TOKENIZE', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_tokenize()
print 'Validated TOKENIZE'
 
args = ['python', os.path.join(py_scripts_path, 'transform.py'), '-i', 'us_states', '-o', 'length', '-f', 'state_name', '-n', 'length', '-t', 'org.apache.pig.piggybank.evaluation.string.LENGTH', '-k']
return_code = subprocess.call(args)
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
validate_length()
print 'Validated LENGTH'

print 'Cleaning up all the temp tables & their schema definitions'
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    for temp in TEMP_TABLES:
        try:
            hbase_client.connection.delete_table(temp, disable=True)
            print 'deleted table',temp
        except:
            pass
        try:
            table = hbase_client.connection.table(CONFIG_PARAMS['hbase_schema_table'])
            table.delete(temp)#also remove the schema info
        except:
            pass
   
print '###########################'
print 'DONE Validating String Functions'
print '###########################'