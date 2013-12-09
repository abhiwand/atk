--This script should be run from the top level directory
REGISTER target/graphbuilder-2.0alpha-with-deps.jar;

x = LOAD 'tutorial/data/employees.csv' USING PigStorage(',') AS (id: int, name: chararray, age: int, dept: chararray, managerId: int, tenure: chararray);
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements2('-v "name=age,managerId" -e "name,dept,worksAt,tenure"');
pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));
dump pge;

