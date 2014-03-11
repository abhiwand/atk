##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
import os
import ast
import sys
import hashlib
from java.util import Properties

from intel_analytics.table.pig import pig_helpers

try:
    from org.apache.pig.scripting import Pig
except ImportError, e:
    print "Pig is either not installed or not executing through Jython. Pig is required for this module."

from intel_analytics.config import global_config as config
from intel_analytics.table.pig.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7
from interval import Interval, Smallest, Largest

def add_quotes(val):
    """
    Add single quote characters around val
    """
    return '\'' + val + '\''

def escape_single_quotes(val):
    """
    replace all single quotes with escape character i.e. 'a' becomes \'a\'
    """
    return val.replace('\'', '\\\'')

def generate_interval_check(column_name, interval):
    """
    generates a AND separated expression to check an interval
    for example: interval = [2..3] returns '( 2 <= column_name AND column_name <= 3 ? '[2..3]': interval) AS interval:chararray'
    """
    expression = []
    if interval.lower_bound not in [Smallest(), Largest()]:
        if isinstance(interval.lower_bound, str):
            bound = add_quotes(interval.lower_bound) 
        else:
            bound = interval.lower_bound
        if interval.lower_closed:
            expression.append('%s <= %s' % (bound, column_name))
        else:
            expression.append('%s < %s' % (bound, column_name))

    if interval.upper_bound not in [Smallest(), Largest()]:
        if isinstance(interval.upper_bound, str):
            bound = add_quotes(interval.upper_bound) 
        else:
            bound = interval.upper_bound
        if interval.upper_closed:
            expression.append('%s <= %s' % (column_name, bound))
        else:
            expression.append('%s < %s' % (column_name, bound))

    condition = " AND ".join(expression)
    suffix = [' ? \'', escape_single_quotes(str(interval)), '\' : interval) AS interval:chararray']
    result = ['( ', condition]
    result.extend(suffix)
    return "".join(result)

def replace_inf(str):
    """
    replaces inf from a string appropriately based on bounds
    """
    return str.replace('-Inf', 'Smallest()').replace('Inf', 'Largest()')

def get_histogram_filename(interval, table_name, column_name):
    if not interval:
        return table_name + '_' + column_name + '_histogram'
    else:
        md5sum = hashlib.md5(interval).hexdigest()
        return table_name + '_' + column_name + '_' + md5sum + '_histogram'


def main(argv):
    parser = ArgumentParser(description='applies feature transformations to features in a big dataset')
    parser.add_argument('-i', '--input', dest='input', help='the input HBase table', required=True)
    parser.add_argument('-n', '--features', dest='feature_names', help='name of the features as a comma separated string')
    parser.add_argument('-t', '--feature_types', dest='feature_types', help='type of the features as a comma separated string')    
    parser.add_argument('-g', '--feature_data_groups', dest='feature_data_groups', help='feature data groups as a pound(#) separated string')    
    parser.add_argument('-o', '--inmemory_computation', dest='inmemory_computation', help='csv flag for each feature whether to do inmemory processing')    

    cmd_line_args = parser.parse_args()
    
    features = [(f.strip()) for f in cmd_line_args.feature_names.split(',')]
    types = [(f.strip()) for f in cmd_line_args.feature_types.split(',')]
    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names)
    feature_data_groups = [(f.strip()) for f in cmd_line_args.feature_data_groups.split('#')]
    inmemory_computation = [eval(f.strip()) for f in cmd_line_args.inmemory_computation.split(',')]

    columns = list(features)

    pig_statements = []
    pig_statements.append("REGISTER %s;" % (config['feat_eng_jar']))
    pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script

    datafu_jar = os.path.join(config['pig_lib'], 'datafu-0.0.10.jar')
    pig_statements.append("REGISTER %s; -- for the VAR UDF" % datafu_jar)
    pig_statements.append("DEFINE VAR datafu.pig.stats.VAR();")
    pig_statements.append("SET default_parallel %s;" % (config['pig_parallelism_factor']))
 
    if len(columns) > 1:
        pig_statements.append("hbase_data_initial = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s') as (%s);" \
                          % (cmd_line_args.input, hbase_constructor_args, pig_schema_info))
    else:
        pig_statements.append("data_single_column_%s = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s') as (%s);" \
                          % (columns[0], cmd_line_args.input, hbase_constructor_args, pig_schema_info))

    binding_variables = {}

    is_comparable  = lambda x: True if x in ['int', 'long', 'double', 'float'] else False

    for t,c in enumerate(columns):

        if inmemory_computation[t]:
            binding_variables['HIST_%s' % (c)] = get_histogram_filename(False, cmd_line_args.input, c)
            if len(columns) > 1:
                pig_statements.append("data_single_column_%s = FOREACH hbase_data_initial GENERATE %s;" % (c,c))
            pig_statements.append("grouped_data2_%s = GROUP data_single_column_%s BY %s;" % (c,c,c))
            pig_statements.append("histogram_%s = FOREACH grouped_data2_%s GENERATE group, COUNT_STAR(data_single_column_%s);" % (c,c,c))
            pig_statements.append("STORE histogram_%s INTO '$HIST_%s';" % (c, c))

        else:
            binding_variables['STAT_%s' % (c)] = cmd_line_args.input + '_' + c + '_stats'
            binding_variables['HIST_%s' % (c)] = get_histogram_filename(feature_data_groups[t], cmd_line_args.input, c)
            
            if len(columns) > 1:
                pig_statements.append("data_single_column_%s = FOREACH hbase_data_initial GENERATE %s;" % (c,c))
    
            pig_statements.append("group_%s = GROUP data_single_column_%s ALL PARALLEL 1;" % (c,c))
    
            if is_comparable(types[t]):
                pig_statements.append("result_%s = FOREACH group_%s { unique_values = DISTINCT data_single_column_%s.%s; GENERATE 'max=', MAX(data_single_column_%s.%s), '\\nmin=', MIN(data_single_column_%s.%s), '\\navg=', AVG(data_single_column_%s.%s), '\\nvar=', VAR(data_single_column_%s.%s), '\\nstdev=', SQRT(VAR(data_single_column_%s.%s)), '\\nunique_values=', COUNT(unique_values);}" % (c,c,c,c,c,c,c,c,c,c,c,c,c,c))
            else:
                pig_statements.append("result_%s = FOREACH group_%s { unique_values = DISTINCT data_single_column_%s.%s; GENERATE 'unique_values=', COUNT(unique_values); }" % (c,c,c,c))
    
            if not feature_data_groups[t]:
                # Column without any interval specification
                pig_statements.append("grouped_data2_%s = GROUP data_single_column_%s BY %s;" % (c,c,c))
                pig_statements.append("histogram_%s = FOREACH grouped_data2_%s GENERATE group, COUNT_STAR(data_single_column_%s);" % (c,c,c))
            else:
                intervals = [eval(replace_inf(f)) for f in feature_data_groups[t].split(":")]
                if is_comparable(types[t]):
                    pig_statements.append("filter_%s = FOREACH data_single_column_%s GENERATE %s, (%s is null ? '': '[...]') as interval:chararray;" % (c,c,c,c))
                else:
                    pig_statements.append("filter_%s = FOREACH data_single_column_%s GENERATE %s, (%s == '' ? '': '[...]') as interval:chararray;" % (c,c,c,c))
                for i,j in enumerate(intervals):
                    k = generate_interval_check(c,j)
                    pig_statements.append("filter_%s = FOREACH filter_%s GENERATE %s, %s;" % (c,c,c,k))
    
                pig_statements.append("project_filter_%s = FOREACH filter_%s GENERATE interval;" % (c,c))
                pig_statements.append("final_group_%s = GROUP project_filter_%s BY interval;" % (c,c))
                pig_statements.append("histogram_%s = FOREACH final_group_%s GENERATE group, COUNT_STAR(project_filter_%s);" % (c,c,c))
    
            pig_statements.append("STORE result_%s INTO '$STAT_%s';" % (c, c))
            pig_statements.append("STORE histogram_%s INTO '$HIST_%s';" % (c, c))


    pig_script = "\n".join(pig_statements)
    compiled = Pig.compile(pig_script)

    props = Properties()
    optimization_params = {'pig.exec.mapPartAgg':'true', 'pig.exec.mapPartAgg.minReduction':'3'}
    for key,value in optimization_params.iteritems():
        props.setProperty(key,value)

    status_list = compiled.bind(binding_variables).run(props)#without binding anything Pig raises error
    return 0 if all(status.isSuccessful() for status in status_list) else 1

if __name__ == "__main__":
  try:
      rc = main(sys.argv)
      sys.exit(rc)
  except Exception, e:
      print e
      sys.exit(1)
