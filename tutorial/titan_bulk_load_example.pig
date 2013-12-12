/**
* This script should be run from the top level directory
* Demonstrates how to bulk load the Titan graph database
*/

REGISTER target/graphbuilder-2.0alpha-with-deps.jar;
IMPORT 'pig/intel_gb2.pig';

--cleanup titan hbase table-->TODO: PUT THIS LOGIC IN GB
-- you need to have HBase installed
sh echo "disable 'test_graph'" | hbase shell --see hbase-titan-conf.xml
sh echo "drop 'test_graph'" | hbase shell

--prepare temp storage
rmf /tmp/empty
fs -mkdir /tmp/empty
rmf /tmp/tmp_store_1;
rmf /tmp/tmp_store_2;

x = LOAD 'tutorial/data/employees.csv' USING PigStorage(',') as (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
x = FILTER x by id!='';
--GB requires the input data to be in HBase
--we need to append HBase row keys to the input relation 
keyed_x = FOREACH x GENERATE FLATTEN(RowKeyAssignerUDF(*));

--create GB input table
sh echo "disable 'gb_input_table'" | hbase shell
sh echo "drop 'gb_input_table'" | hbase shell
sh echo "create 'gb_input_table', {NAME=>'cf'}" | hbase shell --cf is the column family

STORE keyed_x INTO 'hbase://gb_input_table' 
  		USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:id cf:name cf:age cf:dept cf:manager cf:underManager');
	  		
LOAD_TITAN('gb_input_table', '"cf:id=cf:name,cf:age,cf:dept" "cf:manager"',
			   '"cf:id,cf:manager,worksUnder,cf:underManager"',
			   'tutorial/hbase-titan-conf.xml');