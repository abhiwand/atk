/* Copyright (C) 2014 Intel Corporation.
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
 * This script demonstrates one of the many ways to bulk load the Titan graph database.
 * <p>
 * This script assumes it is being called from the Graph Builder home directory.
 * You can override at the command line with "pig -param GB_HOME=/path/to/graphbuilder"
 * </p>
 */
%default GB_HOME '.'

IMPORT '$GB_HOME/pig/graphbuilder.pig';

rmf /tmp/empty_file_to_end_pig_action
rmf /tmp/empty_file_to_start_pig_action
fs -mkdir /tmp/empty_file_to_start_pig_action

employees = LOAD 'examples/data/employees.csv' USING PigStorage(',') AS
		(employee_id:int, name:chararray, age:int, dept:chararray, manager:int, underManager:chararray);


-- Customize the way property graph elements are created from raw input
-- and build a undirected graph with the -e argument
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v employee_id=name,age,dept manager -d employee_id,manager,worksUnder,underManager"');
pge = FOREACH employees GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements

merged = MERGE_DUPLICATE_ELEMENTS(pge); -- merge the duplicate vertices and edges

STORE merged INTO '/tmp/employees_sequencefile' USING  com.intel.pig.store.GraphElementSequenceFile();

-- -O flag specifies overwriting the input Titan table
STORE_GRAPH_INFER_SCHEMA(merged, '$GB_HOME/examples/hbase-titan-conf.xml', '-O');

