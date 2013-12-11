--This script should be run from the top level directory
-- it demonstrates the use of the CreatePropGraphElements  and MergeDuplicateGraphElements by reading from a sample data
-- source and dumping out the the stream of property graph elements - with no duplicates.

REGISTER target/graphbuilder-2.0alpha-with-deps.jar;
IMPORT 'pig/intel_gb2.pig';

x = LOAD 'tutorial/data/employees.csv' USING PigStorage(',') AS (id: int, name: chararray, age: int, dept: chararray, managerId: int, tenure: chararray);
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "name=age,managerId" -e "name,dept,worksAt,tenure"');
pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));

merged = MergeDuplicateGraphElements(pge);
DESCRIBE merged;
dump merged;


