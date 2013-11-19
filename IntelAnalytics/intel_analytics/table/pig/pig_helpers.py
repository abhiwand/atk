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