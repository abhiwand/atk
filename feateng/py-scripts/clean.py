import sys
import re
import subprocess
from tribeca_etl.hbase_client import ETLHBaseClient
from tribeca_etl.argparse_lib import ArgumentParser
from tribeca_etl.config import CONFIG_PARAMS


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
    parser.add_argument('-p', '--parallelism', dest='degree_of_parallelism', help='degree of parallelism (number of reducers in MR jobs)', required=True)
    parser.add_argument('-r', '--replace', dest='replacement_value', help='value to replace the missing values for the given feature (available special constants: avg)')
    parser.add_argument('-a', '--any', dest='should_clean_any', help='clean all rows with a missing value for any of the features', action='store_true', default=False)

    cmd_line_args = parser.parse_args()
    print cmd_line_args

    errors = validate_args(cmd_line_args)
    if len(errors)>0:
        raise Exception(errors) 
    
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        
        if not hbase_client.is_table_readable(cmd_line_args.input):
            raise Exception("%s is not readable. Please make sure the table exists and is enabled.")

        #create if output table doesn't exist, if the output is the same as the input, the input table will get
        #updated as the same keys are used while writing the output
        if not hbase_client.is_table_readable(cmd_line_args.output):          
            hbase_client.drop_create_table(cmd_line_args.output , [CONFIG_PARAMS['etl-column-family']])
        
        columns = hbase_client.get_column_names(cmd_line_args.input, [CONFIG_PARAMS['etl-column-family']])
        stripped_column_names = []
        for c in columns:
            stripped_column_names.append(re.sub(CONFIG_PARAMS['etl-column-family'],'',c))#remove the col. family identifier
        
        schema_information = ''
        for i, c in enumerate(stripped_column_names):
            schema_information += c
            if i != len(stripped_column_names) - 1:
                schema_information += ', '
    
    if cmd_line_args.feature_to_clean and cmd_line_args.feature_to_clean not in stripped_column_names:
        raise Exception("Feature %s does not exist in table %s " % (cmd_line_args.feature_to_clean, cmd_line_args.input))
       
    args = ['pig', 'py-scripts/tribeca_etl/pig/pig_clean.py', '-i', cmd_line_args.input, 
                     '-o', cmd_line_args.output, '-p', cmd_line_args.degree_of_parallelism, '-s', schema_information]
    
    if cmd_line_args.feature_to_clean:  
        args += ['-f', cmd_line_args.feature_to_clean]
        
    if cmd_line_args.should_clean_any:  
        args += ['-a',  str(cmd_line_args.should_clean_any)]
        
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
