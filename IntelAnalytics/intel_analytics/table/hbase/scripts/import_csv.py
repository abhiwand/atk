import subprocess
import commands
import os
import sys

from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.table.pig.argparse_lib import ArgumentParser
from intel_analytics.table.hbase.schema import ETLSchema
from intel_analytics.config import global_config as config


base_script_path = os.path.dirname(os.path.abspath(__file__))

#If the INTEL_ANALYTICS_ETL_RUN_LOCAL env. variable is set, run in local mode
#useful when running the validation tests, which take quite a lot of time if not run in local mode
should_run_local_mode = False
try:
    value = os.environ["INTEL_ANALYTICS_ETL_RUN_LOCAL"]
    if value == 'true':
        should_run_local_mode = True
        print "Will run pig in local mode"
except:
    pass

#hadoop fs count returns DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME
N_COLS=4
CONTENT_SIZE_COL=2
FILE_NAME_COL=3

def main(argv):
    parser = ArgumentParser(description='import.py imports a big dataset from HDFS to HBase')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)
    parser.add_argument('-d', '--delimeter', dest='input_delimeter_char', help='delimeter to use while parsing the input file. Default value is comma')
    parser.add_argument('-s', '--schema', dest='schema_information', help='schema information', required=True)
    parser.add_argument('-k', '--skip_header', dest='skip_header', help='skip the header line (first line) of the CSV file while loading', action='store_true', default=False)

    cmd_line_args = parser.parse_args()
    print cmd_line_args
    
    #check whether the file is empty or not
    out = commands.getoutput('hadoop fs -count %s' % (cmd_line_args.input))
    lines = out.split('\n')#split to lines
    for line in lines:
        splitted = line.split()
        if len(splitted)==N_COLS:
            if cmd_line_args.input in splitted[FILE_NAME_COL]:#got the file name
                file_size = long(splitted[CONTENT_SIZE_COL])#get the file size
                if file_size == 0:
                    raise Exception('The input CSV file is empty')
        
    etl_schema = ETLSchema()
    etl_schema.populate_schema(cmd_line_args.schema_information)
    etl_schema.save_schema(cmd_line_args.output)
    
    feature_names_as_str = etl_schema.get_feature_names_as_CSV()
    feature_types_as_str = etl_schema.get_feature_types_as_CSV()
    
    # need to delete/create output table so that we can write the transformed features
    with ETLHBaseClient() as hbase_client:
        hbase_client.drop_create_table(cmd_line_args.output,
                                       [config['hbase_column_family']])

    import_csv_script_path = os.path.join(config['pig_py_scripts'],
                                          'pig_import_csv.py')

    args = ['pig']
    
    if should_run_local_mode:
        args += ['-x', 'local']
            
    args += [import_csv_script_path,'-i', cmd_line_args.input, 
            '-o', cmd_line_args.output, '-f', feature_names_as_str, '-t', feature_types_as_str]
    
    if cmd_line_args.skip_header:  
        args += ['-k']  
        
    if cmd_line_args.input_delimeter_char:  
        args += ['-d', cmd_line_args.input_delimeter_char]          
    
    subprocess.call(args)

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)