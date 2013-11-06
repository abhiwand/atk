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
    parser.add_argument('-t', '--transformation', dest='transformation_function', help='transformation function to apply to given feature')#if the transformation is not supported, the pig_transform.py will complain about it
    parser.add_argument('-a', '--transformation-args', dest='transformation_function_args', help='Comma separated transformation function arguments. For example, for a feature x to calculate the square of x as a new feature, you should specify the arguments -f x -t POW -a 2')
    parser.add_argument('-n', '--new-feature-name', dest='new_feature_name', help='create a new feature with the given name and with the values obtained from the transformation')
    parser.add_argument('-k', '--keep-original', dest='keep_original_feature', help='whether to keep the original feature (specified with -f) when writing the transformed output', action='store_true', default=False)
    parser.add_argument('-s', '--print-schema', dest='print_schema', help='prints the schema of the given hbase table and exits', action='store_true', default=False)
    
    cmd_line_args = parser.parse_args()
    print cmd_line_args
    
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        if not hbase_client.is_table_readable(cmd_line_args.input):
            print "Specified input table %s is not readable"%(cmd_line_args.input)
            sys.exit(1)    

    etl_schema = ETLSchema()
    etl_schema.load_schema(cmd_line_args.input)
    
    feature_names_as_str = ",".join(etl_schema.feature_names)
    feature_types_as_str = ",".join(etl_schema.feature_types)
    
    #print the schema info from the input hbase table
    if cmd_line_args.print_schema:
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
            for i,feature_name in enumerate(etl_schema.feature_names):
                feature_type = etl_schema.feature_types[i] 
                print "%s:%s"%(feature_name,feature_type)
        sys.exit(1)
        
    if (cmd_line_args.input == cmd_line_args.output) and (not cmd_line_args.keep_original_feature):#in-place transformation AND don't keep source
        raise Exception("For in-place transformations the source/original feature has to be kept")
   
    errors = validate_args(cmd_line_args)
    if len(errors)>0:
        raise Exception(errors)    
    
    if cmd_line_args.take_a_diff:
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
            if cmd_line_args.output and not hbase_client.is_table_readable(cmd_line_args.output):
                print "Specified output table %s is not readable"%(cmd_line_args.output)
                sys.exit(1)        
                
            dest_etl_schema = ETLSchema()
            dest_etl_schema.load_schema(cmd_line_args.output)           
            input_columns = set(etl_schema.feature_names)
            output_columns = set(dest_etl_schema.feature_names)
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
            hbase_client.drop_create_table(cmd_line_args.output, [CONFIG_PARAMS['etl-column-family']])
    
    transform_script_path = os.path.join(base_script_path, 'intel_analytics', 'etl', 'pig', 'pig_transform.py')
        
    args = ['pig']
    
    if should_run_local_mode:
        args += ['-x', 'local']
                               
    args += [transform_script_path, '-f', cmd_line_args.feature_to_transform, 
                '-i', cmd_line_args.input, '-o', cmd_line_args.output, 
                '-t', cmd_line_args.transformation_function, '-n', cmd_line_args.new_feature_name, 
                '-u', feature_names_as_str, '-r', feature_types_as_str]
    
    if cmd_line_args.transformation_function_args:  
        args += ['-a', cmd_line_args.transformation_function_args]
        
    if cmd_line_args.keep_original_feature:  
        args += ['-k']        
    
    #start the pig process
    ret = subprocess.call(args)
    
    if ret == 0:#success
        #need to update schema here as it's difficult to pass the updated schema info from jython to python
        if not cmd_line_args.keep_original_feature:
            if not cmd_line_args.output == cmd_line_args.input:#if NOT an in place transform (the output table is the same as the input table)
                #if the transform is an inplace transform the feature is NOT removed from the source table!
                etl_schema.feature_names.remove(cmd_line_args.feature_to_transform)
        etl_schema.feature_names.append(cmd_line_args.new_feature_name)
        #for now make the new feature bytearray, because all UDF's have different return types
        #and we cannot know their return types
        etl_schema.feature_types.append('bytearray')
        etl_schema.save_schema(cmd_line_args.output)

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)
