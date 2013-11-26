import os
import ast
import sys

from org.apache.pig.scripting import Pig
from intel_analytics.config import global_config as config
from intel_analytics.table.pig.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7
from intel_analytics.table.pig import pig_helpers
from intel_analytics.table.builtin_functions import available_builtin_functions


def generate_hbase_store_args(features, cmd_line_args):
    hbase_store_args = ''#for storing, we need to update the transformed field's name when storing
    cf = config['hbase_column_family']
    for i, f in enumerate(features):
        if cmd_line_args.feature_to_transform == f:
            if cmd_line_args.keep_original_feature:#should also write the original feature to output
                hbase_store_args += '%s ' % ((cf+f))
        else:
            hbase_store_args += '%s ' % ((cf+f))
    
    hbase_store_args += (cf+cmd_line_args.new_feature_name)
    return hbase_store_args

def generate_transform_statement(features, cmd_line_args):
    transform_statement = ''
    for i, f in enumerate(features):
        if cmd_line_args.feature_to_transform == f:
            if cmd_line_args.keep_original_feature:#should also write the original feature to output
                transform_statement += f 
                transform_statement += ', ' 
        else:
            transform_statement += f
            transform_statement += ', ' 

    if cmd_line_args.is_standardization:
        transform_statement += ("(%s - avg_var_relation.$0)/stddev_relation.stddev as %s"%(cmd_line_args.feature_to_transform, cmd_line_args.new_feature_name))
    else:#PIG functions
        #we have some args to pass to the transformation_function
        if cmd_line_args.transformation_function_args:
            transform_statement += "%s(%s," % (cmd_line_args.transformation_function, cmd_line_args.feature_to_transform)
            for i, arg in enumerate(cmd_line_args.transformation_function_args):
                if type(arg) is str:#need to wrap it in quotes for pig
                    transform_statement+="'"
                    transform_statement+=arg#.replace('\\','\\\\') #need to escape backslashes as the concat method interprets backslashes in a regexp & removes one, we need to re-escape them
                    transform_statement+="'"
                else:
                    transform_statement+=str(arg)
                if i != len(cmd_line_args.transformation_function_args) - 1:
                    transform_statement+=','
            transform_statement+=") as %s" % (cmd_line_args.new_feature_name)
            
        else:#without args
            transform_statement += "%s(%s) as %s" % (cmd_line_args.transformation_function, cmd_line_args.feature_to_transform, cmd_line_args.new_feature_name)
    return transform_statement
                      
    
def main(argv):
    parser = ArgumentParser(description='applies feature transformations to features in a big dataset')
    parser.add_argument('-f', '--feature', dest='feature_to_transform', help='the feature to apply transformation to', required=True)
    parser.add_argument('-i', '--input', dest='input', help='the input HBase table', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output HBase table', required=True)
    parser.add_argument('-t', '--transformation', dest='transformation_function', help='transformation function to apply to given feature. Available transformations: %s' % (available_builtin_functions) , required=True)
    parser.add_argument('-a', '--transformation-args', dest='transformation_function_args', help='Transformation function arguments as a list, e.g., -a [\"substring\",0]')
    parser.add_argument('-n', '--new-feature-name', dest='new_feature_name', help='create a new feature with the given name and with the values obtained from the transformation', required=True)
    parser.add_argument('-k', '--keep-original', dest='keep_original_feature', help='whether to keep the original feature (specified with -f) when writing the transformed output', action='store_true', default=False)
    parser.add_argument('-u', '--features', dest='feature_names', help='name of the features as a comma separated string')
    parser.add_argument('-r', '--feature_types', dest='feature_types', help='type of the features as a comma separated string')    
    parser.add_argument('-s', '--schema', dest='schema_information', help='schema information')
    
    cmd_line_args = parser.parse_args()
    
    if (cmd_line_args.input == cmd_line_args.output) and (not cmd_line_args.keep_original_feature):#in-place transformation AND don't keep source
        raise Exception("For in-place transformations the source/original feature has to be kept")
    
    features = [(f.strip()) for f in cmd_line_args.feature_names.split(',')]
    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_store_args = generate_hbase_store_args(features, cmd_line_args)

    #if we have some args, convert the string representation of args to a list
    if cmd_line_args.transformation_function_args:
        cmd_line_args.transformation_function_args = ast.literal_eval(cmd_line_args.transformation_function_args)

    #don't forget to add the key we read from hbase, we read from hbase like .... as (key:chararray, ... remaining_features ...), see below
    features.insert(0, 'key')
        
    if cmd_line_args.transformation_function not in available_builtin_functions:
        raise Exception("%s is not supported. Supported functions are %s" % (cmd_line_args.transformation_function, available_builtin_functions))

    if cmd_line_args.transformation_function == "STND":
        cmd_line_args.is_standardization = True
    else:
        cmd_line_args.is_standardization = False

    pig_statements = []
    pig_statements.append("REGISTER %s;" % (config['feat_eng_jar']))
    pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script
    
    if cmd_line_args.is_standardization:#need datafu jar for standardization, which needs VAR UDF
        datafu_jar = os.path.join(config['pig_lib'], 'datafu-0.0.10.jar')
        pig_statements.append("REGISTER %s; -- for the VAR UDF" % datafu_jar)
        pig_statements.append("DEFINE VAR datafu.pig.stats.VAR();")
        
    pig_statements.append("hbase_data = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s', '-loadKey true') as (key:chararray, %s);" \
                          % (cmd_line_args.input, hbase_constructor_args, pig_schema_info))
    
    transform_statement = generate_transform_statement(features, cmd_line_args)
    
    if cmd_line_args.is_standardization:
        pig_statements.append("grp = GROUP hbase_data ALL;")
        pig_statements.append("avg_var_relation = FOREACH grp GENERATE AVG(grp.hbase_data.%s) as average, VAR(grp.hbase_data.%s) as variance;" % (cmd_line_args.feature_to_transform,cmd_line_args.feature_to_transform))
        pig_statements.append("stddev_relation = FOREACH avg_var_relation GENERATE SQRT(variance) as stddev;")
    
    pig_statements.append("transformed_dataset = FOREACH hbase_data GENERATE %s;" % (transform_statement)) 
    pig_statements.append("store transformed_dataset into 'hbase://$OUTPUT' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % (hbase_store_args))
    pig_script = "\n".join(pig_statements)
    compiled = Pig.compile(pig_script)
    status = compiled.bind({'OUTPUT':cmd_line_args.output}).runSingle()#without binding anything Pig raises error
    return 0 if status.isSuccessful() else 1

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)