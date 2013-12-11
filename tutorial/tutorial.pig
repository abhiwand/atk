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

x = LOAD '/etc/passwd' USING PigStorage(':') AS (username:chararray, f1: chararray, f2: chararray, f3:chararray, f4:chararray);
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "f1" "f2" -e "f1,f2,link,f3"');
pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));
DESCRIBE pge;
dump pge;
merged = MergeDuplicateGraphElements(pge);
DESCRIBE merged;
dump merged;
-- rdf_triples = FOREACH dedupd GENERATE TORDF(*);
-- DESCRIBE rdf_triples;
-- DUMP rdf_triples;

-- STORE some_relation INTO '-' USING store_graph();
-- STORE_GRAPH(final_graph, 'hbase://pagerank_edge_list', 'Titan');
