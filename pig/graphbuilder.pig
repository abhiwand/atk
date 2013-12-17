DEFINE ExtractJSONField com.intel.pig.udf.eval.ExtractJSONField();
DEFINE CreateRowKey com.intel.pig.udf.eval.CreateRowKey();

/**
 * Remove duplicate property graphelements from a stream.
 *
 * <p><ul>
 * <li>Vertices with the same IDs are treated as equal</li>
 * <li>Edges with the same label and matching vertex IDs on their source and destination vertices are treated as equal</li>
 * <li>Property lists are merged between duplicates, with conflicts resolved arbitrarily. </li>
 * </ul> </p>
 * @param inPropGraph
 *
*/

DEFINE MERGEDUPLICATEGRAPHELEMENTS(inPropGraph) RETURNS outPropGraph {
  DEFINE GetPropGraphEltID com.intel.pig.udf.eval.GetPropGraphElementID;
  labeled = FOREACH $inPropGraph GENERATE (GetPropGraphEltID(*)), $0;
  grouped = GROUP labeled by $0;
  DEFINE Merge com.intel.pig.udf.eval.MergeDuplicateGraphElements;
  $outPropGraph = FOREACH grouped GENERATE(Merge(*));
};

/**
* LOAD_TITAN macro bulk loads a graph into the Titan graph database. This macro
* is a wrapper for the com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB MapReduce job.
* For the details of the command line arguments @see com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB.
* 
* @param input_hbase_table_name : name of the input HBase table that GraphBuilder (GB) will create the graph from <br/>
* @param  vertex_rule 		    : vertex creation rule <br/>
* @param  edge_rule			    : edge creation rule <br/>
* @param  config_file		    : path to the XML configuration file to be used by GB for bulk loading to Titan <br/>
*/
DEFINE LOAD_TITAN(input_hbase_table_name, vertex_rule, edge_rule, config_file) RETURNS void {
	-- load an empty relation that will be used in the MAPREDUCE operator
	dummy = LOAD '/tmp/empty' USING TextLoader() AS (line:chararray);
	
	-- we do a dummy STORE/LOAD below as the MAPREDUCE operator 
	-- requires a STORE and then a LOAD operation
	
	stored_graph = MAPREDUCE 'target/graphbuilder-2.0-alpha-with-deps.jar' 
	  		STORE dummy INTO '/tmp/tmp_store_1'
	  		LOAD '/tmp/empty' USING TextLoader() AS (line:chararray) 
	  		`com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB -conf $config_file --tablename $input_hbase_table_name --vertices $vertex_rule --edges $edge_rule`;
	  		
	STORE stored_graph INTO '/tmp/tmp_store_2';
};


/**
* LOAD_TITAN_WITH_ARGS macro bulk loads a graph into the Titan graph database and also passes the given
* arguments to GB for further configuration. This macro
* is a wrapper for the com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB MapReduce job.
* For the details of the command line arguments @see com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB.
* 
* @param input_hbase_table_name : name of the input HBase table that GraphBuilder (GB) will create the graph from <br/>
* @param  vertex_rule 		    : vertex creation rule <br/>
* @param  edge_rule			    : edge creation rule <br/>
* @param  config_file		    : path to the XML configuration file to be used by GB for bulk loading to Titan <br/>
* @param other_args				: other command line arguments to TableToGraphDB <br/>
*/
DEFINE LOAD_TITAN_WITH_ARGS(input_hbase_table_name, vertex_rule, edge_rule, config_file, other_args) RETURNS void {
	-- load an empty relation that will be used in the MAPREDUCE operator
	dummy = LOAD '/tmp/empty' USING TextLoader() AS (line:chararray);
	
	-- we do a dummy STORE/LOAD below as the MAPREDUCE operator 
	-- requires a STORE and then a LOAD operation
	
	stored_graph = MAPREDUCE 'target/graphbuilder-2.0-alpha-with-deps.jar' 
	  		STORE dummy INTO '/tmp/tmp_store_1'
	  		LOAD '/tmp/empty' USING TextLoader() AS (line:chararray) 
	  		`com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB -conf $config_file --tablename $input_hbase_table_name --vertices $vertex_rule --edges $edge_rule $other_args`;
	  		
	STORE stored_graph INTO '/tmp/tmp_store_2';
};
