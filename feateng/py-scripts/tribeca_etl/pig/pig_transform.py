import sys
import os
from org.apache.pig.scripting import Pig
from tribeca_etl.config import CONFIG_PARAMS
from tribeca_etl.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7

"""
STND: standardization (see http://en.wikipedia.org/wiki/Feature_scaling#Standardization)
The rest are Pig builtin functions.
"""
SUPPORTED_FUNCTIONS = ['ABS','LOG','LOG10','POW', 'EXP', 'STND']

def generate_pig_schema(features, cmd_line_args):
    feature_types={}
    pig_schema_info = ''
    hbase_constructor_args = ''#for loading
    hbase_store_args = ''#for storing, we need to update the transformed field's name when storing
    for i, f in enumerate(features):
        hbase_constructor_args += (CONFIG_PARAMS['etl-column-family']+f)#will be like etl-cf:timestamp etl-cf:duration etl-cf:event_type etl-cf:method etl-cf:src_tms etl-cf:dst_tms
        if cmd_line_args.feature_to_transform == f:
            pig_schema_info += (f+":double")#feature_to_transform must be double
            feature_types[f]='double'
            if cmd_line_args.keep_original_feature:#should also write the original feature to output
                hbase_store_args += '%s ' % ((CONFIG_PARAMS['etl-column-family']+f))
        else:
            hbase_store_args += '%s ' % ((CONFIG_PARAMS['etl-column-family']+f))
            pig_schema_info += (f+":chararray") # load all stuff as chararray, it is OK as we are importing the dataset
            feature_types[f]='chararray'
        if i != len(features) - 1:
            hbase_constructor_args += ' '
            pig_schema_info += ', '
    
    hbase_store_args += (CONFIG_PARAMS['etl-column-family']+cmd_line_args.new_feature_name)
    return hbase_store_args, hbase_constructor_args, pig_schema_info, feature_types

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
        if cmd_line_args.transformation_function_args:#we have some args to pass to the transformation_function
            if cmd_line_args.is_piggybank_function:#piggybank stuff
                transform_statement += "org.apache.pig.piggybank.evaluation.math.%s(%s,%s) as %s" % (cmd_line_args.transformation_function, cmd_line_args.feature_to_transform, cmd_line_args.transformation_function_args, cmd_line_args.new_feature_name)
            else:#PIG builtin functions with args
                transform_statement += "%s(%s,%s) as %s" % (cmd_line_args.transformation_function, cmd_line_args.feature_to_transform, cmd_line_args.transformation_function_args, cmd_line_args.new_feature_name)
        else:#PIG builtin functions without args
            transform_statement += "%s(%s) as %s" % (cmd_line_args.transformation_function, cmd_line_args.feature_to_transform, cmd_line_args.new_feature_name)
    return transform_statement
                      
    
def main(argv):
    parser = ArgumentParser(description='applies feature transformations to features in a big dataset')
    parser.add_argument('-f', '--feature', dest='feature_to_transform', help='the feature to apply transformation to', required=True)
    parser.add_argument('-i', '--input', dest='input', help='the input HBase table', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output HBase table', required=True)
    parser.add_argument('-p', '--parallelism', dest='degree_of_parallelism', help='degree of parallelism (number of reducers in MR jobs)', required=True, type=int)
    parser.add_argument('-t', '--transformation', dest='transformation_function', help='transformation function to apply to given feature. Available transformations: %s' % (SUPPORTED_FUNCTIONS) , required=True)
    parser.add_argument('-a', '--transformation-args', dest='transformation_function_args', help='transformation function arguments. Currently only POW has a single argument, which is the exponent of the POW function. For example, for a feature x to calculate the square of x as a new feature, you should specify the arguments -f x -t POW -a 2')
    parser.add_argument('-n', '--new-feature-name', dest='new_feature_name', help='create a new feature with the given name and with the values obtained from the transformation', required=True)
    parser.add_argument('-k', '--keep-original', dest='keep_original_feature', help='whether to keep the original feature (specified with -f) when writing the transformed output', action='store_true', default=False)
    parser.add_argument('-s', '--schema', dest='schema_information', help='schema information')
    
    cmd_line_args = parser.parse_args()
    print cmd_line_args

    features = [(f.strip()) for f in cmd_line_args.schema_information.split(',')]#schema_information will be like 'timestamp, item_id, method, src_tms, event_type, dst_tms, duration'
    hbase_store_args, hbase_constructor_args, pig_schema_info, feature_types = generate_pig_schema(features, cmd_line_args)
    
    #don't forget to add the key we read from hbase, we read from hbase like .... as (key:chararray, ... remaining_features ...), see below
    features.insert(0, 'key')
    feature_types['key'] = 'chararray'
        
    #TODO: validate transformation_function_args based on transformation_function
    if cmd_line_args.transformation_function not in SUPPORTED_FUNCTIONS:
        raise Exception("%s is not supported. Supported functions are %s" % (cmd_line_args.transformation_function, SUPPORTED_FUNCTIONS)) 
    
    if cmd_line_args.transformation_function in ["POW"]: # check if function is in piggybank.jar
        cmd_line_args.is_piggybank_function = True
    else:
        cmd_line_args.is_piggybank_function = False
    
    if cmd_line_args.transformation_function == "STND":
        cmd_line_args.is_standardization = True
    else:
        cmd_line_args.is_standardization = False

    pig_statements = []
    pig_statements.append("REGISTER %s; -- Tribeca ETL helper UDFs" % (CONFIG_PARAMS['tribeca-etl-jar']))
    
    if cmd_line_args.is_standardization:#need datafu jar for standardization, which needs VAR UDF
        pig_statements.append("REGISTER %s; -- for the VAR UDF" % (CONFIG_PARAMS['datafu-jar']))
        pig_statements.append("DEFINE VAR datafu.pig.stats.VAR();")

    if cmd_line_args.is_piggybank_function:
        pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script
        
    pig_statements.append("SET default_parallel %s; -- set the number of reducers" % (cmd_line_args.degree_of_parallelism))
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