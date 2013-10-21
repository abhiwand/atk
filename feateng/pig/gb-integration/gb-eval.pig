--calling GB as a UDF and use a custom store function to write the graph to a graph db

REGISTER $FEATURE_ENGINEER_LIB;

import '../feature_engineering/pig/gb-integration/common.pig';

DEFINE store_graph com.intel.pig.gb.GraphDBStore('titan');--our store function is parametrized with the target graph DB

-- the log file below is not actually used, since generate_graph is a dummy eval function
-- we are demonstrating here a UDF where we return dummy graph elements
-- the output schema of the eval function is {(src_vertex: chararray, dest_vertex: chararray, edge_data: chararray, vertex_id: chararray, vertex_data: chararray, element_type: chararray)}
-- element_type is used in FILTER statements to get edges/vertices separately

-- make sure that /tmp/sample.log is in HDFS
logs = LOAD '/tmp/sample.log' USING PigStorage('\n') AS (line: chararray);

graph_elements = FOREACH logs GENERATE generate_graph();

edges = FILTER graph_elements BY ($0.element_type=='edge');
vertices = FILTER graph_elements BY ($0.element_type=='vertex');

first10_edges = LIMIT edges 10;
DUMP first10_edges;

first10_vertices = LIMIT vertices 10;
DUMP first10_vertices;

-- (a dummy store func), in practice this should write the edges/vertices to the graph database specified in the DEFINE statement at the top of the script
STORE edges INTO '-' USING store_graph();
STORE vertices INTO '-' USING store_graph();  