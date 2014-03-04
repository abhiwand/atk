##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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
from intel_analytics.config import global_config as config


def get_pig_schema(feature_names, feature_types):
    """
    Enumerate the features and types list to contruct the schema string that
    is understood by Pig.
    """
    pig_schema = ','.join([x + ":" + y for x,y in zip(feature_names, feature_types)])
    return pig_schema

def get_hbase_storage_schema(feature_names):
    """
    Enumerate the features and types list to contruct the schema string that
    is understood by HBase.
    """
    cf = config['hbase_column_family']
    hbase_schema = ' '.join([cf + x for x in feature_names])
    return hbase_schema

def get_pig_schema_string(feature_names_as_str, feature_types_as_str):
    """
    Returns a schema string in Pig's format given a comma separated feature
    names and types string
    """
    feature_names = feature_names_as_str.split(',')
    feature_types = feature_types_as_str.split(',')

    return get_pig_schema(feature_names, feature_types)

def get_hbase_storage_schema_string(feature_names_as_str):
    """
    Returns the schema string in HBaseStorage's format given a comma-separated
    feature names and types string
    """
    feature_names = feature_names_as_str.split(',')
    return get_hbase_storage_schema(feature_names)

def get_load_statement_list(files, raw_load_statement, out_relation):
    """
    Returns the list of load statements

    Parameters
        ----------
        files : list
            list of file path.
        raw_load_statement : String
            raw statement which needs to take a file path to finish.
            The statement looks similar to:
                LOAD '%s' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',') AS (state:chararray);
        out_relation : String
            The resulting relation name for the loading.
            It will be the relation for subsequent operation such as storing into data store.

    """
    relationship_names = []
    load_statements = []
    for i, file in enumerate(files):
        relation = 'relations_' + str(i)
        relationship_names.append(relation)
        load_statement = raw_load_statement % (file)
        load_statement = relation + ' = ' + load_statement
        load_statements.append(load_statement)
    if len(relationship_names) > 1:
        load_statements.append("%s = UNION %s;" % (out_relation, ','.join(relationship_names)))
    elif len(relationship_names) == 1:
        load_statements.append("%s = %s;" % (out_relation, relationship_names[0]))
    return load_statements

def get_generate_key_statements(in_relation, out_relation, features, offset = 0):
    """
    Return the list of load statements that generate row key

    Parameters
        ----------
        in_relation : String
            The input relation which does not contain row key
        out_relation : String
            The output relation which has row key assigned
        features : String
            Comma separated features names such as f1, f2, f3
        offset : long
            The previous maximum row key

    """
    statements = []
    if offset == 0:
        statements.append('%s = rank %s;' %(out_relation, in_relation))
    else:
        statements.append('temp = rank %s;' %(in_relation))
        statements.append('%s = foreach temp generate $0 + %s as key, %s;' %(out_relation, str(offset), features))

    return statements

def generate_hbase_store_args_for_split(features, cmd_line_args):
    cf = config['hbase_column_family']
    hbase_store_args =  " ".join([cf+f for f in features])
    if cmd_line_args.randomize == "True":
        hbase_store_args += ' ' + (cf+cmd_line_args.input_column)
    hbase_store_args += ' ' + (cf+cmd_line_args.output_column)
    return hbase_store_args


def report_job_status(status):
    print 'Pig job status report-Start:'
    input_status = status.getInputStats()
    input_count = 0
    for status in input_status:
        input_count = input_count + status.getNumberRecords()

    print '%s:%s' %('input_count', str(input_count))
    print 'Pig job status report-End:'


