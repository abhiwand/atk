--calling GB as a raw MR job

-- remove both local and HDFS output
rmf /tmp/gb-wiki-output
rmf /tmp/gb-wiki-input

wiki_logs = LOAD '/user/user/wiki-input';

-- GB creates an edge list and a vertex list in its output dir
edge_list = MAPREDUCE '$GB_PATH/graphbuilder-1.0.0-SNAPSHOT-hadoop-job.jar' STORE wiki_logs INTO '/tmp/gb-wiki-input' LOAD '/tmp/gb-wiki-output/edata' 
	AS (src_vertex: chararray, dest_vertex: chararray, edge_value: chararray) `com.intel.hadoop.graphbuilder.demoapps.wikipedia.docwordgraph.CreateWordCountGraph /user/user/wiki-input /tmp/gb-wiki-output`;

first10_edges = LIMIT edge_list 10;	
DUMP  first10_edges;

-- get the vertices of the wiki graph
vertex_list = LOAD '/tmp/gb-wiki-output/vdata' using PigStorage('\n') AS (vertex_id:chararray, vertex_value: chararray);
first10_vertices = LIMIT vertex_list 10;
DUMP  first10_vertices;