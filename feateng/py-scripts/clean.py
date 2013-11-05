import sys
import re
import subprocess
import os
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.argparse_lib import ArgumentParser
from intel_analytics.etl.config import CONFIG_PARAMS
from intel_analytics.etl.schema import ETLSchema

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

SUPPORTED_CLEAN_STRATEGIES = ['any', 'all']

def validate_args(cmd_line_args):
    errors=[]
    if cmd_line_args.feature_to_clean and cmd_line_args.should_clean_any:
        errors.append("Please specify either -f or -a")
    return errors

def main(argv):
    parser = ArgumentParser(description='cleans big datasets')
    parser.add_argument('-f', '--feature', dest='feature_to_clean', help='the feature to clean based on missing values')
    parser.add_argument('-i', '--input', dest='input', help='the input HBase table', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output HBase table', required=True)
    parser.add_argument('-r', '--replace', dest='replacement_value', help='value to replace the missing values for the given feature (available special constants: avg)')
    parser.add_argument('-s', '--strategy', dest='clean_strategy', help='any: if any missing values are present drop that record, all: if all values are missing, drop that record')

    cmd_line_args = parser.parse_args()
    print cmd_line_args

    errors = validate_args(cmd_line_args)
    if len(errors)>0:
        raise Exception(errors) 
    
    etl_schema = ETLSchema()
    etl_schema.load_schema(cmd_line_args.input)
    etl_schema.save_schema(cmd_line_args.output)
    
    feature_names_as_str = ",".join(etl_schema.feature_names)
    feature_types_as_str = ",".join(etl_schema.feature_types)
    
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        if not hbase_client.is_table_readable(cmd_line_args.input):
            raise Exception("%s is not readable. Please make sure the table exists and is enabled.")

        #create if output table doesn't exist, if the output is the same as the input, the input table will get
        #updated as the same keys are used while writing the output
        if not hbase_client.is_table_readable(cmd_line_args.output):          
            hbase_client.drop_create_table(cmd_line_args.output , [CONFIG_PARAMS['etl-column-family']])
        
    if cmd_line_args.feature_to_clean and cmd_line_args.feature_to_clean not in etl_schema.feature_names:
        raise Exception("Feature %s does not exist in table %s " % (cmd_line_args.feature_to_clean, cmd_line_args.input))
    
    clean_script_path = os.path.join(base_script_path, 'intel_analytics', 'etl', 'pig', 'pig_clean.py')
    
    
    args = ['pig']
    
    if should_run_local_mode:
        args += ['-x', 'local']
    
    args += [clean_script_path, '-i', cmd_line_args.input, 
                     '-o', cmd_line_args.output, '-n', feature_names_as_str,
                     '-t', feature_types_as_str]
    
    if cmd_line_args.feature_to_clean:  
        args += ['-f', cmd_line_args.feature_to_clean]
        
    if cmd_line_args.clean_strategy:  
        if not cmd_line_args.clean_strategy in SUPPORTED_CLEAN_STRATEGIES:
            raise Exception("%s is not a supported clean strategy. Supported strategies are %s" % (cmd_line_args.clean_strategy, SUPPORTED_CLEAN_STRATEGIES))
        args += ['-s',  str(cmd_line_args.clean_strategy)]
        
    if cmd_line_args.replacement_value:  
        args += [ '-r' , cmd_line_args.replacement_value]
    
    #start the pig process
    subprocess.call(args)

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)
