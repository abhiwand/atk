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
package com.intel.hadoop.graphbuilder.pipeline.tokenizer.wordcountgraph;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.VertexSchema;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
/**
 * This class handles the configuration time aspects of the graph construction rule (graph tokenizer) that converts
 * wiki pages (presented as strings) into the property graph elements of the word count graph.
 * <p>
 * It is responsible for:
 * <ul>
 * <li> Generating the property graph schema that this graph construction rule generates and other meta-data required
 * at set-up time.</li>
 * </ul>
 * </p>
 *
 * @see GraphBuildingRule
 * @see PropertyGraphSchema
 * @see WordCountGraphTokenizer
 */
public class WordCountGraphBuildingRule implements GraphBuildingRule {
    private PropertyGraphSchema graphSchema;

    private Class                           vidClass       = StringType.class;
    private Class<? extends GraphTokenizer> tokenizerClass = WordCountGraphTokenizer.class;

    /**
     * Allocates and initializes the graph schema for the word count graph.
     *
     * <p>There is one vertex schema: A vertex ID with no properties.</p>
     * <p>There is one edge schema: It has the edge label "contains" and an integer property, "wordCount".</p>
     */
    public WordCountGraphBuildingRule() {
        graphSchema = new PropertyGraphSchema();

        VertexSchema vertexSchema = new VertexSchema();
        graphSchema.addVertexSchema(vertexSchema);

        EdgeSchema     edgeSchema = new EdgeSchema(WordCountGraphTokenizer.CONTAINS);
        PropertySchema propertySchema = new PropertySchema(WordCountGraphTokenizer.WORDCOUNT, Integer.class);
        edgeSchema.getPropertySchemata().add(propertySchema);
        graphSchema.addEdgeSchema(edgeSchema);
    }

    /**
     * Gets the property graph schema for the wordcount graph.
     * @return The property graph schema of the wordcount graph.
     */
    public PropertyGraphSchema getGraphSchema() {
        return graphSchema;
    }

    /**
     * Updates the MR job configuration with the state needed by the {@code WordCountGraphTokenizer}.
     * @param conf  The job configuration that will store the state and be passed to the {@code WordCountGraphTokenizer}.
     */
    public void updateConfigurationForTokenizer(Configuration conf) {
    }

    /**
     * Gets the class of the tokenizer used by the {@code WordCountGraphBuildingRule}.
     * @return The class of the tokenizer used by the {@code WordCountGraphBuildingRule}.
     * @see WordCountGraphTokenizer
     */
    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return tokenizerClass;
    }

    /**
     * Gets the classname of the vertex IDs used by the wordcount graph.
     *
     */
    public Class vidClass() {
        return vidClass;
    }
}
