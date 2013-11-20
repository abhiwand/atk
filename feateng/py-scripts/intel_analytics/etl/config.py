import os
#current directory of this script, needed for locating the UDF jar path
base_script_path = os.path.dirname(os.path.abspath(__file__))
    
CONFIG_PARAMS = {
    'etl-column-family': 'etl-cf:',
    'etl-schema-table' : 'ETL_SCHEMA',
    'datafu-jar': os.path.join(base_script_path, '..', '..', '..', 'lib','datafu-0.0.10.jar'),
    'hbase-host' : 'localhost'
}