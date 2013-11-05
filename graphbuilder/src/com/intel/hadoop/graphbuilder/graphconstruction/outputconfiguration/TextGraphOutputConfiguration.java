

package com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration;

import com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.textgraph.TextGraphMR;

public class TextGraphOutputConfiguration implements OutputConfiguration {
    private GraphGenerationMRJob graphGenerationMRJob;


    public TextGraphOutputConfiguration() {
        graphGenerationMRJob = new TextGraphMR();
    }

    public GraphGenerationMRJob getGraphGenerationMRJob() {
        return graphGenerationMRJob;
    }
}