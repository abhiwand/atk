/**
* This script should be run from the top level directory
* Demonstrates the use of the CreatePropGraphElements by reading from a sample data source 
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;

DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "name=age,managerId" -e "name,dept,worksAt,tenure"');

employees = LOAD 'examples/data/employees.csv' USING PigStorage(',')
				AS (id: int, name: chararray, age: int, dept: chararray, managerId: int, tenure: chararray);
pge = FOREACH employees GENERATE FLATTEN(CreatePropGraphElements(*));