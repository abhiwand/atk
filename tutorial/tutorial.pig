--This script should be run from the top level directory
REGISTER target/graphbuilder-2.0alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

--JSON example
json_data = LOAD 'tutorial/data/tshirts.json' USING TextLoader() AS (json: chararray);
extracted_first_tshirts_price = FOREACH json_data GENERATE *, ExtractJSONField(json, 'Sizes[0].Price') AS price: double;
extracted_num_sizes = FOREACH extracted_first_tshirts_price GENERATE *, ExtractJSONField(json, 'Sizes.size()') AS num_sizes: int;
extracted_first_color = FOREACH extracted_num_sizes GENERATE *, ExtractJSONField(json, 'Colors[0]') AS first_color: chararray;
extracted_cheapest_tshirt_price = FOREACH extracted_first_color GENERATE *, ExtractJSONField(json, 'Sizes.Price.min()') AS cheapest_price: double;
extracted_size_of_expensive_thirts = FOREACH extracted_first_color GENERATE *, ExtractJSONField(json, 'Sizes.findAll{Sizes -> Sizes.Price>90}.Size[0]') AS tshirt_size: chararray;
DUMP extracted_size_of_expensive_thirts;

--XML example
DEFINE XMLLoader com.intel.pig.load.XMLLoader('tshirts');--extract the 'tshirts' element
xml_data = LOAD 'tutorial/data/tshirts.xml' using com.intel.pig.load.XMLLoader('tshirts') AS (xml: chararray);
DUMP xml_data;

REGISTER /usr/local/pig/piggybank.jar;
xml_data = LOAD 'tutorial/data/tshirts.xml' using org.apache.pig.piggybank.storage.XMLLoader('tshirts') AS (xml: chararray);--extract the 'tshirts' element with Piggy Bank's XMLLoader
DUMP xml_data;

x = LOAD '/etc/passwd' USING PigStorage(':') AS (username:chararray, f1: chararray, f2: chararray, f3:chararray, f4:chararray);
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "f1" "f2" -e "f1,f2,link,f3"');
pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));
DESCRIBE pge;
dump pge;
merged = MERGE_DUPLICATE_ELEMENTS(pge);
DESCRIBE merged;
dump merged;


--RDF example
rmf /tmp/rdf_triples;
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "[OWL.People],id=name,age,dept" "[OWL.People],manager" -e "id,manager,OWL.worksUnder,underManager"');
x = LOAD 'tutorial/data/employees.csv' USING PigStorage(',') as (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
x = FILTER x by id!='';
pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));
DEFINE RDF com.intel.pig.udf.eval.RDF('OWL');--specify the namespace to use with the constructor
rdf_triples = FOREACH pge GENERATE FLATTEN(RDF(*));
DESCRIBE rdf_triples;
STORE rdf_triples INTO '/tmp/rdf_triples' USING PigStorage();

--bulk load titan example

--cleanup titan hbase table-->TODO: PUT THIS LOGIC IN GB
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
keyed_x = FOREACH x GENERATE FLATTEN(CreateRowKey(*));

--create GB input table
sh echo "disable 'gb_input_table'" | hbase shell
sh echo "drop 'gb_input_table'" | hbase shell
sh echo "create 'gb_input_table', {NAME=>'cf'}" | hbase shell --cf is the column family

STORE keyed_x INTO 'hbase://gb_input_table' 
  		USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:id cf:name cf:age cf:dept cf:manager cf:underManager');
	  		
	  		
LOAD_TITAN('gb_input_table', '"cf:id=cf:name,cf:age,cf:dept" "cf:manager"',
			   '"cf:id,cf:manager,worksUnder,cf:underManager"',
			   'tutorial/hbase-titan-conf.xml');