/* Copyright (C) 2013 Intel Corporation.
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
 * This script demonstrates use of JSONPath queries to process JSON data
 * and use of XMLLoader to load XML data
 * <p>
 * This script assumes it is being called from the Graph Builder home directory.
 * You can override at the command line with "pig -param GB_HOME=/path/to/graphbuilder"
 * </p>
 */
%default GB_HOME '.'

IMPORT '$GB_HOME/pig/graphbuilder.pig';


json_data = LOAD 'examples/data/tshirts.json' USING TextLoader() AS (json: chararray);
extracted_first_tshirts_price = FOREACH json_data GENERATE *, ExtractJSONField(json, 'Sizes[0].Price') AS price: double;
extracted_num_sizes = FOREACH extracted_first_tshirts_price GENERATE *, ExtractJSONField(json, 'Sizes.size()') AS num_sizes: int;
extracted_first_color = FOREACH extracted_num_sizes GENERATE *, ExtractJSONField(json, 'Colors[0]') AS first_color: chararray;
extracted_cheapest_tshirt_price = FOREACH extracted_first_color GENERATE *, ExtractJSONField(json, 'Sizes.Price.min()') AS cheapest_price: double;
extracted_size_of_expensive_thirts = FOREACH extracted_cheapest_tshirt_price GENERATE *, ExtractJSONField(json, 'Sizes.findAll{Sizes -> Sizes.Price>90}.Size[0]') AS tshirt_size: chararray;
DUMP extracted_size_of_expensive_thirts;

DEFINE XMLLoader com.intel.pig.load.XMLLoader('tshirts');--extract the 'tshirts' element
xml_data = LOAD 'examples/data/tshirts.xml' using com.intel.pig.load.XMLLoader('tshirts') AS (xml: chararray);
DUMP xml_data;
