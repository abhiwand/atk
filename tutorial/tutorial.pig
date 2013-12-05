--TODO: don't use abs path
REGISTER '/home/nyigitba/workspace/graphbuilder-2/target/graphbuilder-2.0alpha-with-deps.jar';

DEFINE ExtractJSON com.intel.pig.udf.eval.ExtractJSON();
DEFINE store_graph com.intel.pig.store.RDFStoreFunc('arguments');

json_data = load 'data/books.json' using TextLoader() as (json: chararray);
extracted_first_books_author = FOREACH json_data GENERATE *, ExtractJSON(json, 'store.book[0].author') as author: chararray;
extracted_price = FOREACH extracted_first_books_author GENERATE *, ExtractJSON(json, 'store.book.findAll{book -> book.price}[0].price') as price: double;
extracted_num_books = FOREACH extracted_price GENERATE *, ExtractJSON(json, 'store.book.size()') as num_books: int;
extracted_some_boolean_field = FOREACH extracted_num_books GENERATE *, ExtractJSON(json, 'store.book[0].boolean_field') as my_boolean: boolean;
final_relation = FOREACH extracted_some_boolean_field GENERATE author, price, num_books, my_boolean; -- get rid of the json field
filtered = FILTER final_relation BY my_boolean == '';

x = load '/etc/passwd' using PigStorage(':') as (username:chararray, f1: chararray, f2: chararray, f3:chararray, f4:chararray);
tokenized = foreach x generate com.intel.pig.udf.eval.ExtractElement(*); 
describe tokenized;
dump tokenized;

STORE some_relation INTO '-' USING store_graph();
