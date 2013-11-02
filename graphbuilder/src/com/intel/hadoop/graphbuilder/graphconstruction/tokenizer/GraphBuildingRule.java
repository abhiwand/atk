package com.intel.hadoop.graphbuilder.graphconstruction.tokenizer;

import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertyGraphSchema;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;

public interface GraphBuildingRule {

    public void    updateConfigurationForTokenizer (Configuration configuration, CommandLine cmd);
    /**
     * @return the property graph schema for the graph for the graph elements generated
     */
    public PropertyGraphSchema getGraphSchema();

    public Class<? extends GraphTokenizer> getGraphTokenizerClass();

    /**
     * @return Class of the VidType. Used for type safety in the high level.
     */
    Class vidClass();
}
