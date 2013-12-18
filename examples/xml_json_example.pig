/**
* This script should be run from the top level directory
* Demonstrates use of JSONPath queries to process JSON data 
* and use of XMLLoader to load XML data
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

json_data = LOAD 'examples/data/tshirts.json' USING TextLoader() AS (json: chararray);
extracted_first_tshirts_price = FOREACH json_data GENERATE *, ExtractJSONField(json, 'Sizes[0].Price') AS price: double;
extracted_num_sizes = FOREACH extracted_first_tshirts_price GENERATE *, ExtractJSONField(json, 'Sizes.size()') AS num_sizes: int;
extracted_first_color = FOREACH extracted_num_sizes GENERATE *, ExtractJSONField(json, 'Colors[0]') AS first_color: chararray;
extracted_cheapest_tshirt_price = FOREACH extracted_first_color GENERATE *, ExtractJSONField(json, 'Sizes.Price.min()') AS cheapest_price: double;
extracted_size_of_expensive_thirts = FOREACH extracted_cheapest_tshirt_price GENERATE *, ExtractJSONField(json, 'Sizes.findAll{Sizes -> Sizes.Price>90}.Size[0]') AS tshirt_size: chararray;
DUMP extracted_size_of_expensive_thirts;

DEFINE XMLLoader com.intel.pig.load.XMLLoader('tshirts');--extract the 'tshirts' element
xml_data = LOAD 'examples/data/tshirts.xml' using com.intel.pig.load.XMLLoader('tshirts') AS (xml: chararray);
DUMP xml_data;

-- make sure /usr/local/pig/piggybank.jar exists
REGISTER /usr/local/pig/piggybank.jar;
xml_data = LOAD 'examples/data/tshirts.xml' using org.apache.pig.piggybank.storage.XMLLoader('tshirts') AS 
				(xml: chararray);--extract the 'tshirts' element with Piggy Bank's XMLLoader
DUMP xml_data;