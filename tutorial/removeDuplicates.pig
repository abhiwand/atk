--This script should be run from the top level directory
-- it demonstrates the use of the CreatePropGraphElements  and MergeDuplicateGraphElements by reading from a sample data
-- source and dumping out the the stream of property graph elements - with no duplicates.

REGISTER target/graphbuilder-2.0alpha-with-deps.jar;

x = LOAD 'tutorial/data/employees.csv' USING PigStorage(',') AS (id: int, name: chararray, age: int, dept: chararray, managerId: int, tenure: chararray);
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "name=age,managerId" -e "name,dept,worksAt,tenure"');
pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));
DEFINE getPropGraphEltID com.intel.pig.udf.eval.GetPropGraphEltID;
pgeLabeled = FOREACH pge GENERATE (getPropGraphEltID(*)), $0;
-- dump pgeLabeled;
grouped = GROUP pgeLabeled by $0;
dump grouped;
DESCRIBE grouped;
DEFINE merge com.intel.pig.udf.eval.MergeDuplicateGraphElements;
merged = FOREACH grouped GENERATE(merge(*));
DESCRIBE merged;
dump merged;


