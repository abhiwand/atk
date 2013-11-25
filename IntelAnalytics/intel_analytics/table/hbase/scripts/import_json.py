import subprocess
import os
import sys

from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.table.pig.argparse_lib import ArgumentParser
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

def main(argv):
    parser = ArgumentParser(description='import.py imports a big dataset from HDFS to HBase')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)

    cmd_line_args = parser.parse_args()
    print cmd_line_args

    # need to delete/create output table so that we can write the transformed features
    with ETLHBaseClient() as hbase_client:
        hbase_client.drop_create_table(cmd_line_args.output,
                                       [config['hbase_column_family']])
    
    import_json_script_path = os.path.join(config['pig_py_scripts'],
                                           'pig_import_json.py')

    args = ['pig']
    
    if should_run_local_mode:
        args += ['-x', 'local']
            
    args += [import_json_script_path, '-i', cmd_line_args.input, '-o', cmd_line_args.output]
    subprocess.call(args)

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)