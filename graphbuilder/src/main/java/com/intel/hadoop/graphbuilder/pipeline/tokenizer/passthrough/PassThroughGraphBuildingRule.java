/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.tokenizer.passthrough;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class handles the configuration time aspects of the graph construction
 * rule (graph tokenizer) that passes through serialized graph elements from a sequence file
 * to the Titan loader.
 *
 * @see GraphBuildingRule
 * @see PropertyGraphSchema
 * @see com.intel.hadoop.graphbuilder.pipeline.tokenizer.passthrough.PassThroughTokenizer
 */

public class PassThroughGraphBuildingRule implements GraphBuildingRule {

    private PropertyGraphSchema graphSchema;
    private final Class vidClass = StringType.class;
    private final Class<? extends GraphTokenizer> tokenizerClass = PassThroughTokenizer.class;

    /**
     * Allocates and initializes the property graph schema.
     *
     * @param propTypeMap    : map of property names to datatype classes
     * @param edgeSignatures : a map of edge label names to lists of property names
     */
    public PassThroughGraphBuildingRule(HashMap<String, Class<?>> propTypeMap,
                                        HashMap<String, ArrayList<String>> edgeSignatures) {

        graphSchema = new PropertyGraphSchema(propTypeMap, edgeSignatures);
    }

    /**
     * Gets the property graph schema used when loading the sequence file.
     *
     * @return The property graph schema used when loading the sequence file.
     */
    public PropertyGraphSchema getGraphSchema() {
        return graphSchema;
    }

    /**
     * Updates the Map Reduce configuration for use by the {@code PassThroughTokenizer}.
     *
     * @param conf  The hadoop configuration being updated.
     */
    public void updateConfigurationForTokenizer(Configuration conf) {
    }

    /**
     * Gets the class of the {@code GraphTokenizer} used to construct the  graph.
     *
     * @return The class of the {@code GraphTokenizer} used to construct the  graph.
     * @see PassThroughTokenizer
     */
    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return tokenizerClass;
    }

    /**
     * Gets the class of vertex IDs used to construct the link graph.
     *
     * @return The class of vertex IDs used to construct the link graph.
     * @see StringType
     */
    public Class vidClass() {
        return vidClass;
    }
}
