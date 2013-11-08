import sys
from org.apache.pig.scripting import Pig
from tribeca_etl.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7
from tribeca_etl.config import CONFIG_PARAMS


def generate_hbase_storage_arguments(features, cmd_line_args):
    feature_types={}
    pig_schema_info = ''
    hbase_constructor_args = ''
    for i, f in enumerate(features):
        hbase_constructor_args += (CONFIG_PARAMS['etl-column-family']+f)#will be like etl-cf:timestamp etl-cf:duration etl-cf:event_type etl-cf:method etl-cf:src_tms etl-cf:dst_tms
        if (cmd_line_args.replacement_value == 'avg') and (f == cmd_line_args.feature_to_clean):
                pig_schema_info += (f+":double")#feature_to_clean must be double
                feature_types[f]='double'
        else:
            pig_schema_info += (f+":chararray") # load all stuff as chararray, it is OK as we are cleaning the dataset
            feature_types[f]='chararray'
        if i != len(features) - 1:
                hbase_constructor_args += ' '
                pig_schema_info += ', '
    return hbase_constructor_args, pig_schema_info, feature_types

def main(argv):
    parser = ArgumentParser(description='cleans a big dataset')
    parser.add_argument('-f', '--feature', dest='feature_to_clean', help='the feature to clean based on missing values')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output file path (on HDFS)', required=True)
    parser.add_argument('-p', '--parallelism', dest='degree_of_parallelism', help='degree of parallelism (number of reducers in MR jobs)', required=True, type=int)
    parser.add_argument('-r', '--replace', dest='replacement_value', help='value to replace the missing values for the given feature (available special constants: avg)')
    parser.add_argument('-a', '--any', dest='should_clean_any', help='clean all rows with a missing value for any of the features')
    parser.add_argument('-s', '--schema', dest='schema_information', help='schema information')
    
    cmd_line_args = parser.parse_args()
        
    features = [(f.strip()) for f in cmd_line_args.schema_information.split(',')]#schema_information will be like 'timestamp, item_id, method, src_tms, event_type, dst_tms, duration'
    
    hbase_constructor_args, pig_schema_info, feature_types = generate_hbase_storage_arguments(features, cmd_line_args)
    
    #don't forget to add the key we read from hbase, we read from hbase like .... as (key:chararray, ... remaining_features ...), see below
    features.insert(0, 'key')
    feature_types['key'] = 'chararray'
    
    
    pig_statements = []
    pig_statements.append("SET default_parallel %s; -- set the number of reducers" % (cmd_line_args.degree_of_parallelism))
    pig_statements.append("parsed_val = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s', '-loadKey true') as (key:chararray, %s);" \
                          % (cmd_line_args.input, hbase_constructor_args, pig_schema_info))
    pig_statements.append("all_records_group = GROUP parsed_val ALL;")
    
    final_relation_to_store = 'cleaned_data'
    if cmd_line_args.feature_to_clean:
        if feature_types[cmd_line_args.feature_to_clean] == 'chararray':#chararray checks are done using empty strings instead of NULLs
            pig_statements.append("cleaned_data = FILTER parsed_val BY (%s != '');" % (cmd_line_args.feature_to_clean))
            pig_statements.append("null_relation = FILTER parsed_val BY (%s == '');" % (cmd_line_args.feature_to_clean))
        else:
            pig_statements.append("cleaned_data = FILTER parsed_val BY %s is not NULL;" % (cmd_line_args.feature_to_clean))
            pig_statements.append("null_relation = FILTER parsed_val BY %s is NULL;" % (cmd_line_args.feature_to_clean))
    elif cmd_line_args.should_clean_any:
        any_clean_statement=''
        for i, feature in enumerate(features):
          any_clean_statement += "(%s != '')" % (feature)#chararray checks are done using empty strings instead of NULLs
          if i != len(features) - 1:
            any_clean_statement += ' and '    
        pig_statements.append("cleaned_data = FILTER parsed_val BY %s;" % any_clean_statement)

    if cmd_line_args.replacement_value:
        generate_statement = ''
        for i, feature in enumerate(features):
          if cmd_line_args.feature_to_clean == feature:
              if cmd_line_args.replacement_value == 'avg':
                  generate_statement += 'AVG(all_records_group.parsed_val.%s) as average' % (cmd_line_args.feature_to_clean)
              else:
                  generate_statement += "TRIM('%s') as %s:chararray" % (cmd_line_args.replacement_value, cmd_line_args.feature_to_clean)
          else:
              generate_statement += "$%d" % i
          if i != len(features) - 1:
              generate_statement += ","
              
        final_relation_to_store = 'replaced_cleaned'
        pig_statements.append("replaced_null_relation = FOREACH null_relation GENERATE %s;" % generate_statement)
        pig_statements.append("replaced_cleaned = UNION replaced_null_relation, cleaned_data;")
    
    pig_statements.append("store %s into 'hbase://$OUTPUT' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % (final_relation_to_store, hbase_constructor_args))
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
