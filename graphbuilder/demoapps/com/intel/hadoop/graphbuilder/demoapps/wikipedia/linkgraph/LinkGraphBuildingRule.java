package com.intel.hadoop.graphbuilder.demoapps.wikipedia.linkgraph;

import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.VertexSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;


public class LinkGraphBuildingRule implements GraphBuildingRule {

    private PropertyGraphSchema    graphSchema;

    public LinkGraphBuildingRule() {
        graphSchema = new PropertyGraphSchema();

        VertexSchema vertexSchema = new VertexSchema();
        graphSchema.addVertexSchema(vertexSchema);

        EdgeSchema edgeSchema = new EdgeSchema(LinkGraphTokenizer.LINKSTO);
        graphSchema.addEdgeSchema(edgeSchema);
    }


    public PropertyGraphSchema getGraphSchema() {
        return graphSchema;
    }

    public void updateConfigurationForTokenizer(Configuration conf, CommandLine cmd) {
    }

    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return LinkGraphTokenizer.class;
    }

    public Class vidClass() {
        return StringType.class;
    }
}
