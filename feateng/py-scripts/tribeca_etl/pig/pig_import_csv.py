import sys
import os
from org.apache.pig.scripting import Pig
from tribeca_etl.config import CONFIG_PARAMS
from tribeca_etl.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7
from tribeca_etl.pig_helpers import generate_pig_schema

def main(argv):
    parser = ArgumentParser(description='imports a big CSV dataset from HDFS to HBase')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)
    parser.add_argument('-p', '--parallelism', dest='degree_of_parallelism', help='degree of parallelism (number of reducers in MR jobs)', required=True, type=int)
    parser.add_argument('-s', '--schema', dest='schema_information', help='schema information')
    parser.add_argument('-k', '--skip_header', dest='skip_header', help='skip the header line (first line) of the CSV file while loading', action='store_true', default=False)

    cmd_line_args = parser.parse_args()
    print cmd_line_args
    
    features = [(f.strip()) for f in cmd_line_args.schema_information.split(',')]#schema_information will be like 'timestamp, item_id, method, src_tms, event_type, dst_tms, duration'
    hbase_constructor_args, pig_schema_info, feature_types = generate_pig_schema(features)
    
    pig_statements = []
    pig_statements.append("SET default_parallel %s; -- set the number of reducers" % (cmd_line_args.degree_of_parallelism))
    if cmd_line_args.skip_header:
        #use an old version of piggybank's csv storage packaged in feature engineering jar file
        pig_statements.append("REGISTER %s; -- Tribeca ETL helper UDFs" % (CONFIG_PARAMS['tribeca-etl-jar']))
        pig_statements.append("logs = LOAD '%s' USING com.tribeca.etl.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (%s);" % (cmd_line_args.input, pig_schema_info))
    else:
        #use piggybank's csv storage
        pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script
        pig_statements.append("logs = LOAD '%s' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (%s);" % (cmd_line_args.input, pig_schema_info))
    pig_statements.append("with_unique_keys = rank logs;" )#prepends row IDs to each row 
    pig_statements.append("STORE with_unique_keys INTO 'hbase://$OUTPUT' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % (hbase_constructor_args))
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
