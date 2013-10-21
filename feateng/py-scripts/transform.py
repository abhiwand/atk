import sys
import re
import subprocess
from tribeca_etl.hbase_client import ETLHBaseClient
from tribeca_etl.argparse_lib import ArgumentParser
from tribeca_etl.config import CONFIG_PARAMS


def validate_args(cmd_line_args):
    errors = []
    if cmd_line_args.take_a_diff:
        if not cmd_line_args.output:
            errors.append("-o/--output is required")
        #if the user is taking a diff, this is the only arg we need
        return errors    
    if not cmd_line_args.feature_to_transform:
        errors.append("-f/--feature is required")    
    if not cmd_line_args.output:
        errors.append("-o/--output is required")
    if not cmd_line_args.degree_of_parallelism:
        errors.append("-p/--parallelism is required")
    if not cmd_line_args.transformation_function:
        errors.append("-t/--transformation is required")
    if not cmd_line_args.new_feature_name:
        errors.append("-n/--new-feature-name is required")  
    return errors                  

def main(argv):
    parser = ArgumentParser(description='applies feature transformations to features in big datasets')
    parser.add_argument('-d', '--diff', dest='take_a_diff', help='show the new/generated features as a result of applying transformations', action='store_true', default=False)
    parser.add_argument('-f', '--feature', dest='feature_to_transform', help='the feature to apply transformation to')
    parser.add_argument('-i', '--input', dest='input', help='the input HBase table', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output HBase table')
    parser.add_argument('-p', '--parallelism', dest='degree_of_parallelism', help='degree of parallelism (number of reducers in MR jobs)')
    parser.add_argument('-t', '--transformation', dest='transformation_function', help='transformation function to apply to given feature')#if the transformation is not supported, the pig_transform.py will complain about it
    parser.add_argument('-a', '--transformation-args', dest='transformation_function_args', help='transformation function arguments. Currently only POW has a single argument, which is the exponent of the POW function. For example, for a feature x to calculate the square of x as a new feature, you should specify the arguments -f x -t POW -a 2')
    parser.add_argument('-n', '--new-feature-name', dest='new_feature_name', help='create a new feature with the given name and with the values obtained from the transformation')
    parser.add_argument('-k', '--keep-original', dest='keep_original_feature', help='whether to keep the original feature (specified with -f) when writing the transformed output', action='store_true', default=False)
    parser.add_argument('-s', '--print-schema', dest='print_schema', help='prints the schema of the given hbase table and exits', action='store_true', default=False)
    
    cmd_line_args = parser.parse_args()
    print cmd_line_args
    
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        if not hbase_client.is_table_readable(cmd_line_args.input):
            print "Specified input table %s is not readable"%(cmd_line_args.input)
            sys.exit(1)    

    #print the schema info from the input hbase table
    if cmd_line_args.print_schema:
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
            columns = hbase_client.get_column_names(cmd_line_args.input, [CONFIG_PARAMS['etl-column-family']])
            stripped_column_names = []
            for c in columns:
                stripped_column_names.append(re.sub(CONFIG_PARAMS['etl-column-family'],'',c))#remove the col. family identifier
            print "Available columns under the '%s' column family: %s" % (CONFIG_PARAMS['etl-column-family'], stripped_column_names)   
            
        sys.exit(1)
   
    errors = validate_args(cmd_line_args)
    if len(errors)>0:
        raise Exception(errors)    
    
    if cmd_line_args.take_a_diff:
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
            if cmd_line_args.output and not hbase_client.is_table_readable(cmd_line_args.output):
                print "Specified output table %s is not readable"%(cmd_line_args.output)
                sys.exit(1)                   
                             
            columns = hbase_client.get_column_names(cmd_line_args.input, [CONFIG_PARAMS['etl-column-family']])
            input_columns = set()
            for c in columns:
                input_columns.add(re.sub(CONFIG_PARAMS['etl-column-family'],'',c))
                
            columns = hbase_client.get_column_names(cmd_line_args.output, [CONFIG_PARAMS['etl-column-family']])
            output_columns = set()
            for c in columns:
                output_columns.add(re.sub(CONFIG_PARAMS['etl-column-family'],'',c))                
                
            diff_columns = list(output_columns - input_columns)
            input_columns = list(input_columns)
            input_columns.sort()
            output_columns = list(output_columns)
            output_columns.sort()
            print "Table %s has columns %s " % (cmd_line_args.input, input_columns)
            print "Table %s has columns %s " % (cmd_line_args.output, output_columns)
            print "Columns generated by transforms:", (diff_columns)
            
        sys.exit(1)
            
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        #create if output table doesn't exist
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
        
    args = ['pig', 'py-scripts/tribeca_etl/pig/pig_transform.py', '-f', cmd_line_args.feature_to_transform, '-i', cmd_line_args.input,
                         '-o', cmd_line_args.output, '-p', cmd_line_args.degree_of_parallelism, '-t', cmd_line_args.transformation_function,
                          '-n', cmd_line_args.new_feature_name, '-s', schema_information]
    
    if cmd_line_args.transformation_function_args:  
        args += ['-a', cmd_line_args.transformation_function_args]
        
    if cmd_line_args.keep_original_feature:  
        args += ['-k']        
    
    #start the pig process
    subprocess.call(args)

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)
