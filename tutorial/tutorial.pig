DEFINE store_graph com.intel.pig.store.RDFStoreFunc('arguments');
register '/home/nyigitba/workspace/graphbuilder-2/target/graphbuilder-1.0.0-SNAPSHOT-with-deps.jar';

x = load '/etc/passwd' using PigStorage(':') as (username:chararray, f1: chararray, f2: chararray, f3:chararray, f4:chararray);
tokenized = foreach x generate com.intel.pig.udf.ExtractElement(*); 

describe tokenized;
dump tokenized;

STORE some_relation INTO '-' USING store_graph();
