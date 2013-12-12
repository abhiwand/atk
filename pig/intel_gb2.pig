DEFINE ExtractJSONField com.intel.pig.udf.eval.ExtractJSONField();
DEFINE CreateRowKey com.intel.pig.udf.eval.CreateRowKey();

/**
* LOAD_TITAN macro bulk loads a graph into the Titan graph database. This macro
* is a wrapper for the com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB MapReduce job.
* For the details of the command line arguments @see com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB.
* 
* input_hbase_table_name : name of the input HBase table that GraphBuilder (GB) will create the graph from <br/>
* vertex_rule 		   : vertex creation rule <br/>
* edge_rule			   : edge creation rule <br/>
* config_file			   : path to the XML configuration file to be used by GB for bulk loading to Titan <br/>
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