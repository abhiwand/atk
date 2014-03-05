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
 * This script assumes it is being called from the Graph Builder home directory.
 * You can override at the command line with "pig -param GB_HOME=/path/to/graphbuilder"
 */
%default GB_HOME '.';
%default GB_JAR '$GB_HOME/target/graphbuilder-2.0-alpha-with-deps.jar';

-- REGISTER doesn't normally use single quotes but they are needed here because of param expansion
REGISTER '$GB_JAR';


DEFINE ExtractJSONField com.intel.pig.udf.eval.ExtractJSONField();
DEFINE CreateRowKey com.intel.pig.udf.eval.CreateRowKey();
DEFINE RegexExtractAllMatches com.intel.pig.udf.eval.RegexExtractAllMatches();
DEFINE FlattenAsGBString com.intel.pig.udf.eval.FlattenAsGBString();
DEFINE NoOpLoad com.intel.pig.udf.load.NoOpLoad();
DEFINE NoOpStore com.intel.pig.udf.store.NoOpStore();

/**
 * Remove duplicates from a given relation of property graph elements.
 *
 * <p><ul>
 * <li>Vertices with the same IDs are treated as equal</li>
 * <li>Edges with the same label and matching vertex IDs on their source and destination vertices are treated as equal</li>
 * <li>Property lists are merged between duplicates, with conflicts resolved arbitrarily. </li>
 * </ul> </p>
 * @param inPropGraph relation of property graph elements to remove the duplicates from
 *
*/

DEFINE MERGE_DUPLICATE_ELEMENTS(inPropGraph) RETURNS outPropGraph {
  DEFINE GetPropGraphEltID com.intel.pig.udf.eval.GetPropGraphElementID;
  labeled = FOREACH $inPropGraph GENERATE (GetPropGraphEltID(*)), $0;
  grouped = GROUP labeled BY $0;
  DEFINE Merge com.intel.pig.udf.eval.MergeDuplicateGraphElements;
  $outPropGraph = FOREACH grouped GENERATE FLATTEN(Merge(*));
};

/**
* STORE_GRAPH macro bulk loads a graph into the Titan graph database and also passes the given
* arguments to GB for further configuration. This macro is a wrapper for the
* com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB MapReduce job.
* For the details of the command line arguments @see com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB.
* 
* @param graphelements          : the set of property graph elements (as a unary relation) to be stored <br/>
* @param config_file		    : path to the XML configuration file to be used by GB for bulk loading to Titan <br/>
* @param property_types         : string defining the Java datatypes of the graph element properties that appear in
*                                 the graphelements stream. It is a comma separated list of the form
*                                 {@code property1:datatype1,property2:datatype2,....} <br/>
* @param edge_schemata          : string defining the edge schemata for the edges that appear in the graphelements
*                                 stream. It is a semi-colon separated list of comma separated lists. Each comma separated
*                                 list specifies an edge label followed by the property names that can be associated with
*                                 an edge of that label. Eg.
*                                 {@code label1,property11,property12;label2;label3,property31;....} <br/>
* @param other_args				: other command line arguments to
*                                 \link com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB \endlink <br/>
*/
DEFINE STORE_GRAPH(graphelements, config_file, property_types, edge_schemata, other_args) RETURNS void {

	-- the MAPREDUCE operator requires a STORE and then a LOAD operation
	-- the STORE  dumps the graphelements into a sequence file for the map reduce task to consume
	-- the LOAD is a dummy operation

    stored_graph = MAPREDUCE '$GB_JAR'
                   STORE $graphelements INTO '/tmp/graphdb_storage_sequencefile' USING  com.intel.pig.store.GraphElementSequenceFile()
                   LOAD '/tmp/load_titan_dummy_location1' USING NoOpLoad()
                   `com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB -conf $config_file -i /tmp/graphdb_storage_sequencefile -p $property_types -E $edge_schemata $other_args` ;


    -- Pig will optimize away the Titan load if we don't "STORE" its dataflow output

  	STORE stored_graph INTO '/tmp/load_titan_dummy_location3' USING NoOpStore();
};

/**
* STORE_GRAPH macro bulk loads a graph into the Titan graph database and also passes the given
* arguments to GB for further configuration. This macro is a wrapper for the
* com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB MapReduce job.
* For the details of the command line arguments @see com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB.
*
* @param graphelements          : the set of property graph elements (as a unary relation) to be stored <br/>
* @param config_file		    : path to the XML configuration file to be used by GB for bulk loading to Titan <br/>
* @param other_args				: other command line arguments to
*                                 \link com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB \endlink <br/>
*/
DEFINE STORE_GRAPH_INFER_SCHEMA(graphelements, config_file, other_args) RETURNS void {

	-- the MAPREDUCE operator requires a STORE and then a LOAD operation
	-- the STORE  dumps the graphelements into a sequence file for the map reduce task to consume
	-- the LOAD is a dummy operation

    stored_graph = MAPREDUCE '$GB_JAR'
                   STORE $graphelements INTO '/tmp/graphdb_storage_sequencefile' USING  com.intel.pig.store.GraphElementSequenceFile()
                   LOAD '/tmp/load_titan_dummy_location1' USING NoOpLoad()
                   `com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB -conf $config_file -i /tmp/graphdb_storage_sequencefile -I $other_args` ;


    -- Pig will optimize away the Titan load if we don't "STORE" its dataflow output

  	STORE stored_graph INTO '/tmp/load_titan_dummy_location3' USING NoOpStore();
};

/**
* LOAD_TITAN macro bulk loads a graph into the Titan graph database and also passes the given
* arguments to GB for further configuration. This macro
* is a wrapper for the com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB MapReduce job.
* For the details of the command line arguments @see com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB.
*
* @param input_hbase_table_name : name of the input HBase table that GraphBuilder (GB) will create the graph from <br/>
* @param  vertex_rule 		    : vertex creation rule \link com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule see HBaseGraphBuildingRule \endlink <br/>
* @param  edge_rule			    : edge creation rule \link com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule see HBaseGraphBuildingRule \endlink 
								  In this edge rule the user also has to specify the direction with a preceding --directedEdges or --edges <br/>
* @param  config_file		    : path to the XML configuration file to be used by GB for bulk loading to Titan <br/>
* @param other_args				: other command line arguments to \link com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB TableToGraphDB \endlink <br/>
*/
DEFINE LOAD_TITAN(input_hbase_table_name, vertex_rule, edge_rule, config_file, other_args) RETURNS void {
	-- load an empty relation that will be used in the MAPREDUCE operator
	dummy = LOAD '/tmp/load_titan_dummy_location1' USING NoOpLoad();
	
	-- we do a dummy STORE/LOAD below as the MAPREDUCE operator 
	-- requires a STORE and then a LOAD operation
	
	stored_graph = MAPREDUCE '$GB_JAR'
	  		STORE dummy INTO '/tmp/load_titan_dummy_location2' USING NoOpStore()
	  		LOAD '/tmp/load_titan_dummy_location1' USING NoOpLoad()
	  		`com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB -conf $config_file $other_args --tablename $input_hbase_table_name --vertices $vertex_rule $edge_rule`;

	STORE stored_graph INTO '/tmp/load_titan_dummy_location3' USING NoOpStore();
};



/**
* LOAD_TITAN_NEW macro bulk loads a graph into the Titan graph database from an HBase table.
* This macro is meant as a replacement for LOAD_TITAN, but it has different arguments.  PAY ATTENTION!
*
* @param input_hbase_table_name : name of the input HBase table that GraphBuilder (GB) will create the graph from <br/>
*                                The name must start with 'hbase://'
* @param full_column_names      : the list of full-column names to be read from HBase (whitespace separated)
* @param schema                 : the tuple schema for the records read from HBase
* @param vertex_rule 		    : vertex creation rule \link com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule see HBaseGraphBuildingRule \endlink <br/>
* @param edge_rule			    : edge creation rule \link com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule see HBaseGraphBuildingRule \endlink <br/>
* @param config_file		    : path to the XML configuration file to be used by GB for bulk loading to Titan <br/>
* @param property_types         : list of property name-datatype pairs to be used in Titan
* @param edge_schemata          : the edge schemata for the Titan graph being generated
* @param other_args				: other command line arguments to \link com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB TableToGraphDB \endlink <br/>
*/
DEFINE LOAD_TITAN_NEW(input_hbase_table_name, full_column_names, schema, vertex_rule, edge_rule, config_file,
                      property_types, edge_schemata, other_args) RETURNS void {

    records = LOAD  '$input_hbase_table_name'
    USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('$full_column_names') AS ($schema);


    -- Customize the way property graph elements are created from raw input
    -- and build a undirected graph with the -e argument

    DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v $vertex_rule -e $edge_rule');

    pge = FOREACH records GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements

    merged = MERGE_DUPLICATE_ELEMENTS(pge); -- merge the duplicate vertices and edges

    stored_graph = MAPREDUCE '$GB_JAR'
                   STORE merged INTO '/tmp/graphdb_storage_sequencefile' USING  com.intel.pig.store.GraphElementSequenceFile()
                   LOAD '/tmp/dummy_location' USING NoOpLoad() AS (line:chararray)
                   `com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB -conf $config_file -i /tmp/graphdb_storage_sequencefile -p $property_types -E $edge_schemata $other_args` ;


    -- Pig will optimize away the Titan load if we don't "STORE" its dataflow output
    -- even though its dataflow "output" is an empty text file

	STORE stored_graph INTO '/tmp/load_titan_dummy_location3' USING NoOpStore();
};

/**
 * GRAPH_UNION macro takes two relations of property graph elements (SerializedGraphElements)
 * and then performs a union and removes duplicates.
 *
 * @param propertyGraph1 relation of property graph elements
 * @param propertyGraph2 relation of property graph elements
 */
DEFINE GRAPH_UNION(propertyGraph1, propertyGraph2) RETURNS propertyGraphUnion {
    withDuplicates = UNION $propertyGraph1, $propertyGraph2;
    $propertyGraphUnion = MERGE_DUPLICATE_ELEMENTS(withDuplicates);
};
