

package com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs;

import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.InputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.HashMap;

/**
 * These are the methods that the (chained) MR job(s) for generating a graph must provide.
 * @see com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.textgraph.TextGraphMR
 * @see  com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.titanwriter.TitanWriterMRChain
 */

public abstract class GraphGenerationMRJob {
    public abstract void setValueClass(Class valueClass);
    public abstract void setVidClass(Class vidClass);
    public abstract void setCleanBidirectionalEdges(boolean clean);
    public abstract void setUserOptions(HashMap<String, String> userOpts);
    public abstract void init(InputConfiguration inputConfiguration, GraphTokenizer tokenizer);
    public abstract void run(CommandLine cmd) throws IOException, ClassNotFoundException, InterruptedException;
}
