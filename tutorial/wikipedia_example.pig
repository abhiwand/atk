/**
* This script should be run from the top level directory
* Demonstrates how to bulk load the Titan graph database
* with the Link-Page graph created from the Wikipedia dataset
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar
IMPORT 'pig/graphbuilder.pig';

sh echo "disable 'test_graph'" | hbase shell --see hbase-titan-conf.xml
sh echo "drop 'test_graph'" | hbase shell

--prepare temp storage
rmf /tmp/empty
fs -mkdir /tmp/empty
rmf /tmp/tmp_store_1;
rmf /tmp/tmp_store_2;

		   
sh echo "disable 'wiki_table'" | hbase shell
sh echo "drop 'wiki_table'" | hbase shell
sh echo "create 'wiki_table', 'features'" | hbase shell			   
			   
DEFINE RegexExtractAllMatches com.intel.pig.udf.eval.RegexExtractAllMatches();
DEFINE FlattenAsGBString com.intel.pig.udf.eval.FlattenAsGBString();

xml_data = LOAD 'wiki_toy_sample.txt' using com.intel.pig.load.XMLLoader('page') AS (page: chararray);
x = FOREACH xml_data GENERATE REGEX_EXTRACT(page, '<id>(.*?)</id>', 1) AS (id: chararray), page;
x = FOREACH x GENERATE REGEX_EXTRACT(page, '<title>(.*?)</title>', 1) AS (title: chararray), id, page;
x = FOREACH x GENERATE REGEX_EXTRACT(page, '<text\\s.*>(.*?)</text>', 1) AS (text: chararray), id, title, page;
x = FOREACH x GENERATE RegexExtractAllMatches(page, '\\[\\[(.*?)\\]\\]') AS (links:bag{}), id, title; --extract all links as a bag
y = FOREACH x GENERATE id, title, FlattenAsGBString(links) AS flattened_links:chararray;--flatten the bag of links in the format GB can process
keyed_y = FOREACH y GENERATE FLATTEN(CreateRowKey(*)); --assign row keys 

STORE keyed_y INTO 'hbase://wiki_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('features:id features:title features:flattened_links');
LOAD_TITAN_WITH_ARGS('wiki_table', '"features:title=features:id" "features:flattened_links"', 
                             '"features:title,features:flattened_links,LINKS"',
                           'tutorial/hbase-titan-conf.xml', '-F');
 
