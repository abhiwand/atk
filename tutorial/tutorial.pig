--This script should be run from the top level directory
REGISTER target/graphbuilder-2.0alpha-with-deps.jar;
IMPORT 'pig/intel_gb2.pig';

--JSON example
json_data = LOAD 'tutorial/data/tshirts.json' USING TextLoader() AS (json: chararray);
extracted_first_tshirts_price = FOREACH json_data GENERATE *, ExtractJSON(json, 'Sizes[0].Price') AS price: double;
extracted_num_sizes = FOREACH extracted_first_tshirts_price GENERATE *, ExtractJSON(json, 'Sizes.size()') AS num_sizes: int;
extracted_first_color = FOREACH extracted_num_sizes GENERATE *, ExtractJSON(json, 'Colors[0]') AS first_color: chararray;
extracted_cheapest_tshirt_price = FOREACH extracted_first_color GENERATE *, ExtractJSON(json, 'Sizes.Price.min()') AS cheapest_price: double;
extracted_size_of_expensive_thirts = FOREACH extracted_first_color GENERATE *, ExtractJSON(json, 'Sizes.findAll{Sizes -> Sizes.Price>90}.Size[0]') AS tshirt_size: chararray;
DUMP extracted_size_of_expensive_thirts;

--XML example
DEFINE XMLLoader com.intel.pig.load.XMLLoader('tshirts');--extract the 'tshirts' element
xml_data = LOAD 'tutorial/data/tshirts.xml' using com.intel.pig.load.XMLLoader('tshirts') AS (xml: chararray);
DUMP xml_data;

REGISTER /usr/local/pig/piggybank.jar;
xml_data = LOAD 'tutorial/data/tshirts.xml' using org.apache.pig.piggybank.storage.XMLLoader('tshirts') AS (xml: chararray);--extract the 'tshirts' element with Piggy Bank's XMLLoader
DUMP xml_data;

--RDF example
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements2('-v "[OWL.People],id=name,age,dept" "[OWL.People],manager" -e "id,manager,OWL.worksUnder,underManager"');
x = LOAD 'tutorial/data/employees.csv' USING PigStorage(',') as (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
x = FILTER x by id!='';
pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));
DEFINE TORDF com.intel.pig.udf.eval.TORDF('OWL');--specify the namespace to use with the constructor
rdf_triples = FOREACH pge GENERATE FLATTEN(TORDF(*));
DESCRIBE rdf_triples;
DUMP rdf_triples;
STORE rdf_triples INTO '/tmp/rdf_triples' USING PigStorage();


-- STORE some_relation INTO '-' USING store_graph();
-- STORE_GRAPH(final_graph, 'hbase://pagerank_edge_list', 'Titan');
 