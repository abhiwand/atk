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

def get_pig_schema_string(feature_names_as_str, feature_types_as_str):
    """
    Returns a schema string in Pig's format given a comma separated feature
    names and types string
    """
    feature_names = feature_names_as_str.split(',')
    feature_types = feature_types_as_str.split(',')
    
    pig_schema = ''
    for i,feature_name in enumerate(feature_names):
        feature_type = feature_types[i] 
        pig_schema += feature_name
        pig_schema += ':'
        pig_schema += feature_type
        if i != len(feature_names)-1:
            pig_schema+=','
    return pig_schema

def get_hbase_storage_schema_string(feature_names_as_str, feature_types_as_str):
    """
    Returns the schema string in HBaseStorage's format given a comma-separated
    feature names and types string
    """
    feature_names = feature_names_as_str.split(',')
    feature_types = feature_types_as_str.split(',')
            
    hbase_storage_schema = ''
    for i,feature_name in enumerate(feature_names):
        feature_type = feature_types[i] 
        hbase_storage_schema += (config['hbase_column_family'] + feature_name)
        if i != len(feature_names)-1:
            hbase_storage_schema+=' '
    return hbase_storage_schema