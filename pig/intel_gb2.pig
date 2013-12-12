--MACROS, DEFINES will go here.

DEFINE ExtractJSON com.intel.pig.udf.eval.ExtractJSON();
DEFINE TORDF com.intel.pig.udf.eval.TORDF();


/**
 * Remove duplicate property graphelements from a stream.
 *
 * <p><ul>
 * <li>Vertices with the same IDs are treated as equal</li>
 * <li>Edges with the same label and matching vertex IDs on their source and destination vertices are treated as equal</li>
 * <li>Property lists are merged between duplicates, with conflicts resolved arbitrarily. </li>
 * </ul> </p>
 *
*/

DEFINE MERGEDUPLICATEGRAPHELEMENTS(inPropGraph) RETURNS outPropGraph {
  DEFINE getPropGraphEltID com.intel.pig.udf.eval.GetPropGraphElementID;
  labeled = FOREACH $inPropGraph GENERATE (getPropGraphEltID(*)), $0;
  grouped = GROUP labeled by $0;
  DEFINE merge com.intel.pig.udf.eval.MergeDuplicateGraphElements;
  $outPropGraph = FOREACH grouped GENERATE(merge(*));
};

DEFINE LOAD_TITAN(edge_list, hbase_out_table, graph_db) RETURNS void {
  stored_graph = MAPREDUCE 'graphbuilder-2.0-hadoop-job.jar' STORE final_graph INTO 'hbase://pagerank_edge_list' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:src_domain cf:dest_domain cf:num_links') LOAD '/tmp/empty' using TextLoader() as (line:chararray) `com.intel.hadoop.graphbuilder.demoapps.tabletographdb.TableToGraphDB -conf hbaseToTitan.xml -t pagerank_edge_list -v "cf:src_domain" "cf:dest_domain" -e "cf:src_domain,cf:dest_domain,link,cf:num_links"`;
  STORE stored_graph INTO '/tmp/tmp_store';
};