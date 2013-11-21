

package com.intel.hadoop.graphbuilder.pipeline.output.textgraph;

import com.intel.hadoop.graphbuilder.pipeline.output.OutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphGenerationMRJob;

public class TextGraphOutputConfiguration implements OutputConfiguration {
    private GraphGenerationMRJob graphGenerationMRJob;


    public TextGraphOutputConfiguration(String outputPathName) {
        graphGenerationMRJob = new TextGraphMR(outputPathName);
    }

    public GraphGenerationMRJob getGraphGenerationMRJob() {
        return graphGenerationMRJob;
    }
}