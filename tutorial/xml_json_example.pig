/**
* This script should be run from the top level directory
* Demonstrates how to manipulate JSON data with JSONPath queries
* and also how to use GB 2.0 (alpha) XMLLoader to load XML data
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/intel_gb2.pig';

json_data = LOAD 'tutorial/data/tshirts.json' USING TextLoader() AS (json: chararray);
extracted_first_tshirts_price = FOREACH json_data GENERATE *, ExtractJSON(json, 'Sizes[0].Price') AS price: double;
extracted_num_sizes = FOREACH extracted_first_tshirts_price GENERATE *, ExtractJSON(json, 'Sizes.size()') AS num_sizes: int;
extracted_first_color = FOREACH extracted_num_sizes GENERATE *, ExtractJSON(json, 'Colors[0]') AS first_color: chararray;
extracted_cheapest_tshirt_price = FOREACH extracted_first_color GENERATE *, ExtractJSON(json, 'Sizes.Price.min()') AS cheapest_price: double;
extracted_size_of_expensive_thirts = FOREACH extracted_first_color GENERATE *, ExtractJSON(json, 'Sizes.findAll{Sizes -> Sizes.Price>90}.Size[0]') AS tshirt_size: chararray;
DUMP extracted_size_of_expensive_thirts;

DEFINE XMLLoader com.intel.pig.load.XMLLoader('tshirts');--extract the 'tshirts' element
xml_data = LOAD 'tutorial/data/tshirts.xml' using com.intel.pig.load.XMLLoader('tshirts') AS (xml: chararray);
DUMP xml_data;

-- make sure /usr/local/pig/piggybank.jar exists
REGISTER /usr/local/pig/piggybank.jar;
xml_data = LOAD 'tutorial/data/tshirts.xml' using org.apache.pig.piggybank.storage.XMLLoader('tshirts') AS (xml: chararray);--extract the 'tshirts' element with Piggy Bank's XMLLoader
DUMP xml_data;