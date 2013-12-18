--This script should be run from the top level directory
-- it demonstrates the use of the CreatePropGraphElements  and MergeDuplicateGraphElements by reading from a sample data
-- source and dumping out the the stream of property graph elements - with no duplicates.

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

x = LOAD 'examples/data/employees.csv' USING PigStorage(',') AS (id: int, name: chararray, age: int, dept: chararray, managerId: int, tenure: chararray);
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "name=age,managerId" -e "name,dept,worksAt,tenure"');
pge = FOREACH x GENERATE FLATTEN(CreatePropGraphElements(*));

rmf /tmp/edgelist;

merged = MERGE_DUPLICATE_ELEMENTS(pge);

DEFINE EdgeList com.intel.pig.udf.eval.EdgeList('true'); -- false means do not print edge properties
edgelist = FOREACH merged GENERATE EdgeList(*);
edgelist = FILTER edgelist BY $0 != '';
DUMP edgelist;
-- STORE edgelist INTO '/tmp/edgelist' USING PigStorage();

DEFINE VertexList com.intel.pig.udf.eval.VertexList('false'); -- false means do not print vertex properties
vertexlist = FOREACH merged GENERATE VertexList(*);
vertexlist = FILTER vertexlist BY $0 != '';
DUMP vertexlist;
