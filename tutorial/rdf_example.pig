/**
* This script should be run from the top level directory
* Demonstrates how to generate RDF triples from property graph elements
*/

REGISTER target/graphbuilder-2.0alpha-with-deps.jar;
IMPORT 'pig/intel_gb2.pig';

rmf /tmp/rdf_triples; --delete the output directory

-- Customize the way property graph elements are created from raw input
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements2('-v "[OWL.People],id=name,age,dept" "[OWL.People],manager" -e "id,manager,OWL.worksUnder,underManager"');

--specify the RDF namespace to use
DEFINE TORDF com.intel.pig.udf.eval.TORDF('OWL');

x = LOAD 'tutorial/data/employees.csv' USING PigStorage(',') as (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
x = FILTER x by id!='';
pge = FOREACH x GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements
rdf_triples = FOREACH pge GENERATE FLATTEN(TORDF(*)); -- generate the RDF triples
DESCRIBE rdf_triples;
STORE rdf_triples INTO '/tmp/rdf_triples' USING PigStorage();