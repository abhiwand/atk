--This script should be run from the top level directory
-- it demonstrates the use of the CreatePropGraphElements by reading from a sample data source and dumping out the
-- the raw stream of property graph elements.

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;

x = LOAD 'examples/data/employees.csv' USING PigStorage(',') AS (id: int, name: chararray, age: int, dept: chararray, managerId: int, tenure: chararray);
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "name=age,managerId" -e "name,dept,worksAt,tenure"');
pge = FOREACH x GENERATE FLATTEN(CreatePropGraphElements(*));
