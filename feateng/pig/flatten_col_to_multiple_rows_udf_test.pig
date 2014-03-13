
/**
* This script is for manually testing the FlattenUDF
*
* This UDF is confusingly named from a Pig perspective because Pig has a built-in called FLATTEN.
* We're calling it flatten here because that is what the Python operation is going to be called.
*
*/

REGISTER target/feature-engineering-0.8-SNAPSHOT-jar-with-dependencies.jar;

DEFINE FlattenPipedAbcs com.intel.pig.udf.flatten.FlattenColumnToMultipleRowsUDF('2', '|', '', '', 'true');

inpt = LOAD 'test-data/flatten-test.csv' USING PigStorage(',') AS (id:int, name:chararray, abcs:chararray);

result = FOREACH inpt GENERATE FLATTEN(FlattenPipedAbcs(*));

STORE result INTO '/tmp/flatten-test-output.csv' USING PigStorage();