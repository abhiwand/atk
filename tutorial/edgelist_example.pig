/**
* This script should be run from the top level directory
* Demonstrates how to generate RDF triples from property graph elements
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/intel_gb2.pig';

rmf /tmp/edgelist; --delete the output directory containing edges
rmf /tmp/vertexlist; --delete the output directory containing vertices

-- Customize the way property graph elements are created from raw input
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "[OWL.People],id=name,age,dept" "[OWL.People],manager" -e "id,manager,OWL.worksUnder,underManager"');

--specify the edge list format ('FALSE' - without properties, 'TRUE' - with properties)
DEFINE TO_EDGELIST com.intel.pig.udf.eval.TO_EDGELIST('false');

--specify the vertex list format ('FALSE' - without properties, 'TRUE' - with properties)
DEFINE TO_VERTEXLIST com.intel.pig.udf.eval.TO_VERTEXLIST('false');

x = LOAD 'tutorial/data/employees.csv' USING PigStorage(',') as (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
x = FILTER x by id!='';

--TODO need to dedup vertices/edges
pge = FOREACH x GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements
vertexlist = FOREACH pge GENERATE TO_VERTEXLIST(*); -- generate the vertex list
filtered_vertices = FILTER vertexlist by $0 != '';--remove the empty tuples, which are created for edges
edgelist = FOREACH pge GENERATE TO_EDGELIST(*); -- generate the edge list
filtered_edges = FILTER edgelist by $0 != '';--remove the empty tuples, which are created for vertices

DESCRIBE filtered_vertices;
DESCRIBE filtered_edges;

STORE filtered_vertices INTO '/tmp/vertexlist' USING PigStorage();
STORE filtered_edges INTO '/tmp/edgelist' USING PigStorage();