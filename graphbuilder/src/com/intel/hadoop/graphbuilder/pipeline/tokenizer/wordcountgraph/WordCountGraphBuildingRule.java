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
 * <p/>
 * <p>
 * It is responsible for:
 * <ul>
 * <li> Generating the property graph schema that this graph construction rule generates and other meta-data required
 * at set-up time</li>
 * </ul>
 * </p>
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
     * <p>There is one edge schema: It has edge label "contains" and an integer property, "wordCount"</p>
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
     * Get the property graph schema for the wordcount graph
     * @return the property graph schema of the wordcount graph
     */
    public PropertyGraphSchema getGraphSchema() {
        return graphSchema;
    }

    /**
     * Update the MR job configuration with state needed by the {@code WordCountGraphTokenizer}
     * @param conf  job configuration that will store the state and be passed to the {@code WordCountGraphTokenizer}
     */
    public void updateConfigurationForTokenizer(Configuration conf) {
    }

    /**
     * Get the class of the tokenizer used by the {@code WordCountGraphBuildingRule}
     * @return the class of the tokenizer used by the {@code WordCountGraphBuildingRule}
     * @see WordCountGraphTokenizer
     */
    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return tokenizerClass;
    }

    /**
     * Get the classname of the vertex IDs used by the wordcount graph
     *
     */
    public Class vidClass() {
        return vidClass;
    }
}
