package com.intel.hadoop.graphbuilder.demoapps.wikipedia.docwordgraph;

import com.intel.hadoop.graphbuilder.demoapps.wikipedia.linkgraph.LinkGraphTokenizer;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.VertexSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;

public class WordCountGraphBuildingRule implements GraphBuildingRule {
    private PropertyGraphSchema graphSchema;

    public WordCountGraphBuildingRule() {
        graphSchema = new PropertyGraphSchema();

        VertexSchema vertexSchema = new VertexSchema();
        graphSchema.addVertexSchema(vertexSchema);

        EdgeSchema     edgeSchema = new EdgeSchema(WordCountGraphTokenizer.CONTAINS);
        PropertySchema propertySchema = new PropertySchema(WordCountGraphTokenizer.WORDCOUNT, Integer.class);
        edgeSchema.getPropertySchemata().add(propertySchema);
        graphSchema.addEdgeSchema(edgeSchema);
    }


    public PropertyGraphSchema getGraphSchema() {
        return graphSchema;
    }

    public void updateConfigurationForTokenizer(Configuration conf, CommandLine cmd) {
    }

    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return WordCountGraphTokenizer.class;
    }

    public Class vidClass() {
        return StringType.class;
    }
}
