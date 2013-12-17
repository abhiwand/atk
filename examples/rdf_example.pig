/**
* This script should be run from the top level directory
* Demonstrates how to generate RDF triples from property graph elements
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

rmf /tmp/rdf_triples; --delete the output directory

-- Customize the way property graph elements are created from raw input
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "[OWL.People],id=name,age,dept" "[OWL.People],manager" -e "id,manager,OWL.worksUnder,underManager"');

--specify the RDF namespace to use
DEFINE RDF com.intel.pig.udf.eval.RDF('OWL');

employees = LOAD 'examples/data/employees.csv' USING PigStorage(',') 
				AS (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
employees_with_valid_ids = FILTER employees BY id!='';
pge = FOREACH employees_with_valid_ids GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements
rdf_triples = FOREACH pge GENERATE FLATTEN(RDF(*)); -- generate the RDF triples
DESCRIBE rdf_triples;
STORE rdf_triples INTO '/tmp/rdf_triples' USING PigStorage();