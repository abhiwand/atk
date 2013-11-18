from tribeca_etl.config import CONFIG_PARAMS

def generate_pig_schema(features):
    feature_types={}
    pig_schema_info = ''
    hbase_constructor_args = ''
    for i, f in enumerate(features):
        hbase_constructor_args += (CONFIG_PARAMS['etl-column-family']+f)#will be like etl-cf:timestamp etl-cf:duration etl-cf:event_type etl-cf:method etl-cf:src_tms etl-cf:dst_tms
        pig_schema_info += (f+":chararray") # load all stuff as chararray, it is OK as we are importing the dataset
        feature_types[f]='chararray'
        if i != len(features) - 1:
            hbase_constructor_args += ' '
            pig_schema_info += ', '
    return hbase_constructor_args, pig_schema_info, feature_types