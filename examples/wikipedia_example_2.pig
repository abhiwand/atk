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
* This script should be run from the top level directory
* Demonstrates how to bulk load the Titan graph database
*/
REGISTER target/graphbuilder-2.0-alpha-with-deps.jar
IMPORT 'pig/graphbuilder.pig';

--prepare temp storage that is used by the LOAD_TITAN macro
--the temp storage is required for doing a dummy LOAD/STORE for the 
--MAPREDUCE operator use in that macro
rmf /tmp/empty
fs -mkdir /tmp/empty
rmf /tmp/tmp_store_1;
rmf /tmp/tmp_store_2;

xml_data = LOAD 'examples/data/wiki_single.txt' using com.intel.pig.load.XMLLoader('page') AS (page: chararray);
id_extracted = FOREACH xml_data GENERATE REGEX_EXTRACT(page, '<id>(.*?)</id>', 1) AS (id: chararray), page;
title_extracted = FOREACH id_extracted GENERATE REGEX_EXTRACT(page, '<title>(.*?)</title>', 1) AS (title: chararray), id, page;
text_extracted = FOREACH title_extracted GENERATE REGEX_EXTRACT(page, '<text\\s.*>(.*?)</text>', 1) AS (text: chararray), id, title, page;
links_extracted = FOREACH text_extracted GENERATE RegexExtractAllMatches(page, '\\[\\[(.*?)\\]\\]') AS (links:bag{}), id, title; --extract all links as a bag 
links_flattened = FOREACH links_extracted GENERATE id, title, FlattenAsGBString(links) AS flattened_links:chararray;--flatten the bag of links in the format GB can process

rmf /tmp/rdf_triples; --delete the output directory
rmf /tmp/edgelist; --delete the output directory containing edges

-- Customize the way property graph elements are created from raw input
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "Title,title=id" "Link,flattened_links" --directedEdges "title,flattened_links,LINKS" -F');

--specify the RDF namespace to use 
DEFINE RDF com.intel.pig.udf.eval.RDF('OWL');

pge = FOREACH links_flattened GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements
merged = MERGE_DUPLICATE_ELEMENTS(pge);
rdf_triples = FOREACH merged GENERATE FLATTEN(RDF(*)); -- generate the RDF triples

--specify the edge list format ('FALSE' - without properties, 'TRUE' - with properties)
DEFINE EdgeList com.intel.pig.udf.eval.EdgeList('false');
edgelist = FOREACH merged GENERATE EdgeList(*); -- generate the edge list for the deduped property graph elements
filtered_edges = FILTER edgelist BY $0 != '';--remove the empty tuples, which are created for vertices

STORE rdf_triples INTO '/tmp/rdf_triples' USING PigStorage();
STORE filtered_edges INTO '/tmp/edgelist' USING PigStorage();