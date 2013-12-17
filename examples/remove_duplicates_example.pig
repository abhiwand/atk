/**
* This script should be run from the top level directory
* Demonstrates the use of the CreatePropGraphElements to created
* graph elements from raw data and use of MergeDuplicateGraphElements 
* to remove duplicate elements
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "name=age,managerId" -e "name,dept,worksAt,tenure"');

employees = LOAD 'examples/data/employees.csv' USING PigStorage(',')
				AS (id: int, name: chararray, age: int, dept: chararray, managerId: int, tenure: chararray);
pge = FOREACH employees GENERATE FLATTEN(CreatePropGraphElements(*));
merged = MERGE_DUPLICATE_ELEMENTS(pge);