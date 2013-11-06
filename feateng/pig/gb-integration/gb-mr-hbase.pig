--calling GB as a raw MR job

--the table first needs to be created in hbase

graph_data = load '/tmp/graph_data.csv' using PigStorage(',') as (id: chararray, name: chararray, age: chararray, dept: chararray, manager: chararray, underManager: chararray);

--hbasestorage uses the first column as the row_key, so need to explicitly create a rowkey
graph_data = foreach graph_data generate id as rowkey: chararray, id, name, age, dept, manager, underManager ; 

--need to load something after an MR job is called, so reload the graph data from hdfs after we are done
graph_data_reloaded = MAPREDUCE '/home/user/nezih/workspace/graphbuilder/target/graphbuilder-2.0.0-hadoop-job.jar' store graph_data INTO 'hbase://pig-hbase-integration' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:id cf:name cf:age cf:dept cf:manager cf:underManager') LOAD '/tmp/graph_data.csv' AS (id: chararray, name: chararray, age: chararray, dept: chararray, manager: chararray, underManager: chararray) `com.intel.hadoop.graphbuilder.demoapps.tribeca.hbase2titangraph.HBaseToTitanGraph --tablename pig-hbase-integration --vertices "cf:id=cf:name,cf:age,cf:dept" --edges "cf:id,cf:manager,isManager,cf:underManager"`;

dump graph_data_reloaded;
