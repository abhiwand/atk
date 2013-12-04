

package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.pipeline.output.OutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphGenerationMRJob;

public class TitanOutputConfiguration implements OutputConfiguration {
    private GraphGenerationMRJob graphGenerationMRJob;


    public TitanOutputConfiguration() {
        graphGenerationMRJob = new TitanWriterMRChain();
    }

    public GraphGenerationMRJob getGraphGenerationMRJob() {
        return graphGenerationMRJob;
    }
}
