

package com.intel.hadoop.graphbuilder.pipeline.output.rdfgraph;

import com.intel.hadoop.graphbuilder.pipeline.output.OutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphGenerationMRJob;

public class RDFGraphOutputConfiguration implements OutputConfiguration {
    private GraphGenerationMRJob graphGenerationMRJob;


    public RDFGraphOutputConfiguration() {
        graphGenerationMRJob = new RDFGraphMR();
    }

    public GraphGenerationMRJob getGraphGenerationMRJob() {
        return graphGenerationMRJob;
    }
}
