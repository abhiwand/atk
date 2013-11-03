package com.intel.hadoop.graphbuilder.demoapps.wikipedia.docwordgraph;

import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.VertexSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
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
 * @see CreateWordCountGraph
 * @see WordCountGraphTokenizer
 */
public class WordCountGraphBuildingRule implements GraphBuildingRule {
    private PropertyGraphSchema graphSchema;

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
     * @param cmd  the command line options provided by the user
     */
    public void updateConfigurationForTokenizer(Configuration conf, CommandLine cmd) {
    }

    /**
     * Get the class of the tokenizer used by the {@code WordCountGraphBuildingRule}
     * @return {@code WordCountGraphTokenizer.class}
     * @see WordCountGraphTokenizer
     */
    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return WordCountGraphTokenizer.class;
    }

    /**
     * Get the classname of the vertex IDs used by the wordcount graph
     * @return {@code StringType.class}
     * @see StringType
     * */
    public Class vidClass() {
        return StringType.class;
    }
}
