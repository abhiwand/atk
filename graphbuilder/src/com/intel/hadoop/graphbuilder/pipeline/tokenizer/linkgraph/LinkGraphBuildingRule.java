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
package com.intel.hadoop.graphbuilder.pipeline.tokenizer.linkgraph;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.VertexSchema;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;


/**
 * This class manages the set-up time state for the graph construction phase that emits vertices and edges of a link
 * graph from a wiki page (presented as a string).
 *
 * <p>
 * <ul>
 *     <li>There is vertex for every wiki page in the specified input file.</li>
 *     <li>There is a "linksTO" edge from each page to every page to which it links.</li>
 * </ul>
 * </p>
 *
 * @see LinkGraphTokenizer
 * @see PropertyGraphSchema
 *
 */
public class LinkGraphBuildingRule implements GraphBuildingRule {

    private PropertyGraphSchema             graphSchema;
    private Class                           vidClass       = StringType.class;
    private Class<? extends GraphTokenizer> tokenizerClass = LinkGraphTokenizer.class;

    /**
     * Allocates and initializes the property graph schema.
     * <p>
     *     <ul>
     *         <li> There is one vertex schema: It has no properties.</li>
     *         <li> There is one edge schema: It has the label "linksTo" and it has no properties.</li>
     *     </ul>
     * </p>
     */
    public LinkGraphBuildingRule() {
        graphSchema = new PropertyGraphSchema();

        VertexSchema vertexSchema = new VertexSchema();
        graphSchema.addVertexSchema(vertexSchema);

        EdgeSchema edgeSchema = new EdgeSchema(LinkGraphTokenizer.LINKSTO);
        graphSchema.addEdgeSchema(edgeSchema);
    }

    /**
     * Gets the property graph schmea for the link graph.
     * @return  The property graph schmea for the link graph.
     */
    public PropertyGraphSchema getGraphSchema() {
        return graphSchema;
    }

    /**
     * Updates the MR configuration for use by the {@code LinkGraphTokenizer}.
     * @param conf The hadoop configuration being updated.
     */
    public void updateConfigurationForTokenizer(Configuration conf) {
    }

    /**
     * Gets the class of the {@code GraphTokenizer} used to construct the link graph.
     * @return  The class of the {@code GraphTokenizer} used to construct the link graph.
     * @see LinkGraphTokenizer
     */
    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return tokenizerClass;
    }

    /**
     * Gets the class of vertex IDs used to construct the link graph.
     * @return  The class of vertex IDs used to construct the link graph.
     * @see StringType
     */
    public Class vidClass() {
        return vidClass;
    }
}
