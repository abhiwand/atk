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
 * This script demonstrates how to bulk load the Titan graph database
 * with the Link-Page graph created from the Wikipedia dataset
 * <p>
 * This script assumes it is being called from the Graph Builder home directory.
 * You can override at the command line with "pig -param GB_HOME=/path/to/graphbuilder"
 * </p>
 */
%default GB_HOME '.'

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar
IMPORT '$GB_HOME/pig/graphbuilder.pig';

--prepare temp storage that is used by the STORE_GRAPH macro
--the temp storage is required for doing a dummy LOAD/STORE for the 
--MAPREDUCE operator use in that macro
-- This annoying. We should get a Jira ticket to fix this in an upcoming Pig release.
rmf /tmp/empty_file_to_end_pig_action
rmf /tmp/empty_file_to_start_pig_action
fs -mkdir /tmp/empty_file_to_start_pig_action

xml_data = LOAD 'examples/data/wiki_single.txt' using com.intel.pig.load.XMLLoader('page') AS (page: chararray);
id_extracted = FOREACH xml_data GENERATE REGEX_EXTRACT(page, '<id>(.*?)</id>', 1) AS (doc_id: chararray), page;
title_extracted = FOREACH id_extracted GENERATE REGEX_EXTRACT(page, '<title>(.*?)</title>', 1) AS (title: chararray), doc_id, page;
text_extracted = FOREACH title_extracted GENERATE REGEX_EXTRACT(page, '<text\\s.*>(.*?)</text>', 1) AS (text: chararray), doc_id, title, page;
links_extracted = FOREACH text_extracted GENERATE RegexExtractAllMatches(page, '\\[\\[(.*?)\\]\\]') AS (links:bag{}), doc_id, title; --extract all links as a bag
links_flattened = FOREACH links_extracted GENERATE doc_id, title, FlattenAsGBString(links) AS flattened_links:chararray;--flatten the bag of links in the format GB can process


-- Customize the way property graph elements are created from raw input
-- and build a directed graph with the --directedEdges argument
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v Title,title=doc_id Link,flattened_links --directedEdges title,flattened_links,LINKS,doc_id flattened_links,title,BACKLINKS -F');


pge = FOREACH links_flattened GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements
merged = MERGE_DUPLICATE_ELEMENTS(pge);

-- -O flag specifies overwriting the input Titan table
-- TODO: -O has to be implemented for Cassandra
STORE_GRAPH(merged, '$GB_HOME/examples/hbase-titan-conf.xml', 'doc_id:String', 'LINKS,doc_id;BACKLINKS', '-O');


