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
    for example: interval = [2..3] returns '2 <= column_name AND column_name <= 3'
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


    return " AND ".join(expression)

def replace_inf(str):
    """
    replaces inf from a string appropriately based on bounds
    """
    return str.replace('-Inf', 'Smallest()').replace('Inf', 'Largest()')


def main(argv):
    parser = ArgumentParser(description='applies feature transformations to features in a big dataset')
    parser.add_argument('-i', '--input', dest='input', help='the input HBase table', required=True)
    parser.add_argument('-n', '--features', dest='feature_names', help='name of the features as a comma separated string')
    parser.add_argument('-t', '--feature_types', dest='feature_types', help='type of the features as a comma separated string')    
    parser.add_argument('-g', '--feature_data_groups', dest='feature_data_groups', help='feature data groups as a pound(#) separated string')    

    cmd_line_args = parser.parse_args()
    
    features = [(f.strip()) for f in cmd_line_args.feature_names.split(',')]
    types = [(f.strip()) for f in cmd_line_args.feature_types.split(',')]
    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names)
    feature_data_groups = [(f.strip()) for f in cmd_line_args.feature_data_groups.split('#')]

    columns = list(features)

    #don't forget to add the key we read from hbase, we read from hbase like .... as (key:chararray, ... remaining_features ...), see below
    features.insert(0, 'key')

    pig_statements = []
    pig_statements.append("REGISTER %s;" % (config['feat_eng_jar']))
    pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script

    datafu_jar = os.path.join(config['pig_lib'], 'datafu-0.0.10.jar')
    pig_statements.append("REGISTER %s; -- for the VAR UDF" % datafu_jar)
    pig_statements.append("DEFINE VAR datafu.pig.stats.VAR();")
    pig_statements.append("SET default_parallel %s;" % (config['pig_parallelism_factor']))
 
    pig_statements.append("hbase_data = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s', '-loadKey true') as (key:chararray, %s);" \
                          % (cmd_line_args.input, hbase_constructor_args, pig_schema_info))

    pig_statements.append("grouped_data1 = GROUP hbase_data ALL PARALLEL 1;")

    binding_variables = {}

    for t,c in enumerate(columns): 
        # Map all the binding variables enlisting output filenames
        binding_variables['STAT_%s' % (c)] = cmd_line_args.input + '_' + c + '_stats'
        if not feature_data_groups[t]:
            binding_variables['HIST_%s' % (c)] = cmd_line_args.input + '_' + c + '_histogram'
        else:
            md5sum = hashlib.md5(feature_data_groups[t]).hexdigest()
            binding_variables['HIST_%s' % (c)] = cmd_line_args.input + '_' + c + '_' + md5sum + '_histogram'

        is_comparable  = lambda x: True if x in ['int', 'long', 'double', 'float'] else False

        # max/min/avg/var/stdev are only valid for comparable datatypes
        if is_comparable(types[t]):
            pig_statements.append("max_result_%s = FOREACH grouped_data1 GENERATE CONCAT('max=', (chararray)MAX(hbase_data.%s));" % (c,c))
            pig_statements.append("min_result_%s = FOREACH grouped_data1 GENERATE CONCAT('min=', (chararray)MIN(hbase_data.%s));" % (c,c))
            pig_statements.append("avg_result_%s = FOREACH grouped_data1 GENERATE CONCAT('avg=', (chararray)AVG(hbase_data.%s));" % (c,c))
            pig_statements.append("var_result_%s = FOREACH grouped_data1 GENERATE CONCAT('var=', (chararray)VAR(hbase_data.%s));" % (c,c))
            pig_statements.append("stdev_result_%s = FOREACH grouped_data1 GENERATE CONCAT('stdev=', (chararray)SQRT(VAR(hbase_data.%s)));" % (c,c))

        pig_statements.append("unique_val_count_%s = FOREACH grouped_data1 { unique_values = DISTINCT hbase_data.%s; GENERATE CONCAT('unique_values=', (chararray)COUNT(unique_values)); }" % (c,c))
    
        pig_statements.append("filter_data1_%s = FOREACH hbase_data GENERATE (%s is null ? 1 : 0) as null_value:int;" % (c,c))
        pig_statements.append("grouped_data2_%s = GROUP filter_data1_%s ALL PARALLEL 1;" % (c,c))
        pig_statements.append("missing_val_count_%s = FOREACH grouped_data2_%s GENERATE CONCAT('missing_values=', (chararray)SUM(filter_data1_%s.null_value));" % (c,c,c))
    

        if is_comparable(types[t]):
            pig_statements.append("result_%s = UNION max_result_%s, min_result_%s, avg_result_%s, var_result_%s, stdev_result_%s, unique_val_count_%s, missing_val_count_%s;" % (c,c,c,c,c,c,c,c))
        else:
            pig_statements.append("result_%s = UNION unique_val_count_%s, missing_val_count_%s;" % (c,c,c))

        if not feature_data_groups[t]:
            # Column without any grouping
            pig_statements.append("grouped_data2_%s = GROUP hbase_data BY %s;" % (c,c))
            pig_statements.append("histogram_%s = FOREACH grouped_data2_%s GENERATE group, COUNT(hbase_data);" % (c,c))
        else:
            intervals = [eval(replace_inf(f)) for f in feature_data_groups[t].split(":")]
            count_variables = []
            for i,j in enumerate(intervals):
                k = generate_interval_check(c,j)
                interval_as_str = escape_single_quotes(str(j))
                pig_statements.append("filter_%s_%d = FILTER hbase_data BY %s;" % (c,i,k))
                pig_statements.append("grouped_data3_%s_%d = GROUP filter_%s_%d ALL PARALLEL 1;" %(c,i,c,i))
                pig_statements.append("count_%s_%d = FOREACH grouped_data3_%s_%d GENERATE '%s', COUNT(filter_%s_%d);" % (c,i,c,i,interval_as_str,c,i))
                count_variables.append("count_%s_%d" % (c,i))
            pig_statements.append("histogram_%s = UNION %s;" % (c,",".join(count_variables)))
            
    
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
