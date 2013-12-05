--MACROS, DEFINES will go here.
DEFINE ExtractJSON com.intel.pig.udf.eval.ExtractJSON();
DEFINE STORE_RDF com.intel.pig.store.RDFStoreFunc('arguments');

DEFINE STORE_GRAPH(edge_list, hbase_out_table, graph_db) RETURNS void {
  stored_graph = MAPREDUCE 'graphbuilder-2.0-hadoop-job.jar' STORE final_graph INTO 'hbase://pagerank_edge_list' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:src_domain cf:dest_domain cf:num_links') LOAD '/tmp/empty' using TextLoader() as (line:chararray) `com.intel.hadoop.graphbuilder.demoapps.tabletographdb.TableToGraphDB -conf hbaseToTitan.xml -t pagerank_edge_list -v "cf:src_domain" "cf:dest_domain" -e "cf:src_domain,cf:dest_domain,link,cf:num_links"`;
  STORE stored_graph INTO '/tmp/tmp_store';
};