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
 * <p>
 * This script assumes it is being called from the Graph Builder home directory.
 * You can override at the command line with "pig -param GB_HOME=/path/to/graphbuilder"
 * </p>
* Demonstrates how to load edge list and vertex list
* that were written in edgelist_example.pig.
*
* Run edgelist_example.pig before this script.
*/
%default GB_HOME '.'

IMPORT '$GB_HOME/pig/graphbuilder.pig';

rmf /tmp/vertexlist2; --delete the output directory containing vertices
rmf /tmp/edgelist2; --delete the output directory containing edges

DEFINE VertexLoad com.intel.pig.udf.load.VertexLoader('false');
DEFINE EdgeLoad com.intel.pig.udf.load.EdgeLoader('false');

--specify the vertex list format ('FALSE' - without properties, 'TRUE' - with properties)
DEFINE VertexList com.intel.pig.udf.eval.VertexList('false');
DEFINE EdgeList com.intel.pig.udf.eval.EdgeList('false');


loaded_vertices = LOAD '/tmp/vertexlist' USING VertexLoad();
loaded_edges = LOAD '/tmp/edgelist' USING EdgeLoad();

vertexlist = FOREACH loaded_vertices GENERATE VertexList(*);
edgelist = FOREACH loaded_edges GENERATE EdgeList(*);

STORE vertexlist INTO '/tmp/vertexlist2' USING PigStorage();
STORE edgelist INTO '/tmp/edgelist2' USING PigStorage();
