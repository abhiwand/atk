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
 * This class manage the set-up time state for the graph construction phase that emits vertices and edges of a link
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
     * Get the property graph schmea for the link graph.
     * @return  the property graph schmea for the link graph
     */
    public PropertyGraphSchema getGraphSchema() {
        return graphSchema;
    }

    /**
     * Update the MR configuration for use by the {@code LinkGraphTokenizer}
     * @param conf hadoop configuration being updated
     * @param cmd  the command line options provided by the user
     */
    public void updateConfigurationForTokenizer(Configuration conf, CommandLine cmd) {
    }

    /**
     * Get the class of the {@code GraphTokenizer} used to construct the link graph
     * @return  the class of the {@code GraphTokenizer} used to construct the link graph
     * @see LinkGraphTokenizer
     */
    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return tokenizerClass;
    }

    /**
     * Get the class of vertex IDs used to construct the link graph.
     * @return  the class of vertex IDs used to construct the link graph.
     * @see StringType
     */
    public Class vidClass() {
        return vidClass;
    }
}
