--TODO: don't use abs path
REGISTER '/home/nyigitba/workspace/graphbuilder-2/target/graphbuilder-2.0alpha-with-deps.jar';

DEFINE ExtractJSON com.intel.pig.udf.eval.ExtractJSON();
DEFINE store_graph com.intel.pig.store.RDFStoreFunc('arguments');

--JSON example
json_data = load 'data/tshirts.json' using TextLoader() as (json: chararray);
extracted_first_tshirts_price = FOREACH json_data GENERATE *, ExtractJSON(json, 'Sizes[0].Price') as price: double;
extracted_num_sizes = FOREACH extracted_first_tshirts_price GENERATE *, ExtractJSON(json, 'Sizes.size()') as num_sizes: int;
extracted_first_color = FOREACH extracted_num_sizes GENERATE *, ExtractJSON(json, 'Colors[0]') as first_color: chararray;
extracted_cheapest_tshirt_price = FOREACH extracted_first_color GENERATE *, ExtractJSON(json, 'Sizes.Price.min()') as cheapest_price: double;
extracted_size_of_expensive_thirts = FOREACH extracted_first_color GENERATE *, ExtractJSON(json, 'Sizes.findAll{Sizes -> Sizes.Price>90}.Size[0]') as tshirt_size: chararray;


--XML example
DEFINE XMLLoader com.intel.pig.load.XMLLoader('tshirts');
xml_data = LOAD 'data/tshirts.xml' using com.intel.pig.load.XMLLoader('tshirts') as (xml: chararray);

REGISTER '/home/nyigitba/pig-0.12.0/contrib/piggybank/java/piggybank.jar';
xml_data = LOAD 'data/tshirts.xml' using org.apache.pig.piggybank.storage.XMLLoader('tshirts') as (xml: chararray);

x = load '/etc/passwd' using PigStorage(':') as (username:chararray, f1: chararray, f2: chararray, f3:chararray, f4:chararray);
tokenized = foreach x generate com.intel.pig.udf.eval.ExtractElement(*); 
describe tokenized;
dump tokenized;

STORE some_relation INTO '-' USING store_graph();
