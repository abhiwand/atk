/**
* This script should be run from the top level directory
* Demonstrates how to bulk load the Titan graph database
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

--prepare temp storage that is used by the LOAD_TITAN macro
--the temp storage is required for doing a dummy LOAD/STORE for the 
--MAPREDUCE operator
rmf /tmp/empty
fs -mkdir /tmp/empty
rmf /tmp/tmp_store_1;
rmf /tmp/tmp_store_2;

x = LOAD 'examples/data/employees.csv' USING PigStorage(',') AS (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
x = FILTER x BY id!='';

--GB requires the input data to be in HBase so
--we need to append HBase row keys to the input relation 
keyed_x = FOREACH x GENERATE FLATTEN(CreateRowKey(*));

--create GB input table
sh echo "disable 'gb_input_table'" | hbase shell
sh echo "drop 'gb_input_table'" | hbase shell
sh echo "create 'gb_input_table', {NAME=>'cf'}" | hbase shell --cf is the column family

STORE keyed_x INTO 'hbase://gb_input_table' 
  		USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:id cf:name cf:age cf:dept cf:manager cf:underManager');
	  		
LOAD_TITAN('gb_input_table', '"cf:id=cf:name,cf:age,cf:dept" "cf:manager"',
			   '"cf:id,cf:manager,worksUnder,cf:underManager"',
			   'examples/hbase-titan-conf.xml', '-O'); -- -O flag specifies overwriting the input Titan table