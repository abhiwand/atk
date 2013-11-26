import sys
import os

from org.apache.pig.scripting import Pig
from intel_analytics.config import global_config as config
from intel_analytics.table.pig.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7

def main(argv):
    """
    Currently we don't get any schema info from the user. We just load
    the whole json as a possibly nested map and dump it as is to HBase.
    """
    parser = ArgumentParser(description='imports a big CSV dataset from HDFS to HBase')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)

    cmd_line_args = parser.parse_args()
    
    pig_statements = []
    pig_statements.append("json_data = LOAD '%s' USING TextLoader() as (json: chararray);" % (cmd_line_args.input))
    pig_statements.append("with_unique_row_keys = RANK json_data;--generate row keys that HBaseStorage needs")
    pig_statements.append("STORE with_unique_row_keys INTO 'hbase://$OUTPUT' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % (config['hbase_column_family']+'json'));
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

