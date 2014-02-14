/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */

/**
* This script should be run from the top level directory.
*
* This script builds on the edgelist example and demonstrates
* how to perform a union on two relations of property graph elements.
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

rmf /tmp/edgelist; --delete the output directory containing edges
rmf /tmp/vertexlist; --delete the output directory containing vertices

-- Customize the way property graph elements are created from raw input
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "[OWL.People],id=name,age,dept" "[OWL.People],manager" -e "id,manager,OWL.worksUnder,underManager"');

--specify the edge list format ('FALSE' - without properties, 'TRUE' - with properties)
DEFINE EdgeList com.intel.pig.udf.eval.EdgeList('true');

--specify the vertex list format ('FALSE' - without properties, 'TRUE' - with properties)
DEFINE VertexList com.intel.pig.udf.eval.VertexList('true');

employees = LOAD 'examples/data/employees.csv' USING PigStorage(',')
				AS (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);

-- employees2 overlaps with employees
employees2 = LOAD 'examples/data/employees2.csv' USING PigStorage(',')
				AS (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);

employees_with_valid_ids = FILTER employees BY id!='';
employees_with_valid_ids2 = FILTER employees2 BY id!='';

pge = FOREACH employees_with_valid_ids GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements
pge2 = FOREACH employees_with_valid_ids2 GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements

-- GRAPH_UNION macro combines these lists of property graph elements, removing duplicates
pge_all = GRAPH_UNION(pge, pge2);

vertexlist = FOREACH pge_all GENERATE VertexList(*); -- generate the vertex list
filtered_vertices = FILTER vertexlist BY $0 != '';--remove the empty tuples, which are created for edges
edgelist = FOREACH pge_all GENERATE EdgeList(*); -- generate the edge list for the deduped property graph elements
filtered_edges = FILTER edgelist BY $0 != '';--remove the empty tuples, which are created for vertices

DESCRIBE filtered_vertices;
DESCRIBE filtered_edges;

STORE filtered_vertices INTO '/tmp/vertexlist' USING PigStorage();
STORE filtered_edges INTO '/tmp/edgelist' USING PigStorage();
