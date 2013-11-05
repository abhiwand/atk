

package com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration;

import com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.titanwriter.TitanWriterMRChain;

public class TitanOutputConfiguration implements OutputConfiguration {
    private GraphGenerationMRJob graphGenerationMRJob;


    public TitanOutputConfiguration() {
        graphGenerationMRJob = new TitanWriterMRChain();
    }

    public GraphGenerationMRJob getGraphGenerationMRJob() {
        return graphGenerationMRJob;
    }
}
