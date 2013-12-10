/* Copyright (C) 2013 Intel Corporation.
*     All rights reserved.
*
 *  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*
* For more about this software visit:
*      http://www.01.org/GraphBuilder
 */

package com.intel.hadoop.graphbuilder.sampleapplications;

import com.intel.hadoop.graphbuilder.pipeline.input.text.textinputformats.WikiPageInputFormat;
import com.intel.hadoop.graphbuilder.pipeline.output.textgraph.TextGraphOutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.OutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanOutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.input.text.TextFileInputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.linkgraph.LinkGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.GraphConstructionPipeline;
import com.intel.hadoop.graphbuilder.util.BaseCLI;
import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.hadoop.graphbuilder.util.Timer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

/**
 * Generates a link graph from a collection of wiki pages.
 * <p>
 * <ul>
 *     <li>There is a vertex for every wiki page in the specified input file.</li>
 *     <li>There is a "linksTO" edge from each page to every page to which it links.</li>
 * </ul>
 * </p>
 *
 * <p>In this release, there are two possible datasinks, a TextGraph, or a load into the Titan graph database. At present,
 * only one datasink can be specified for each run.
 * <ul>
 *     <li>To specify a text output use this option: <code>-o directory_name </code></li>
 *     <li>To specify a Titan load, use this option: <code>-t</code>
 *     <ul><li>Specify the tablename for Titan to use in the config file you specify at: <code> -conf conf_path </code></li>
 *     <li>If you do not specify a tablename, Titan uses the default table name: <code>titan</code></li>
 *     <li>Use the <code>-a</code> option to tell Titan it can append the newly generated graph to an existing
 *         one in the same table. The default behavior is to abort if you try to use an existing Titan table name.</li></ul>
 * </ul>
 * </p>
 *
 */
public class CreateLinkGraph {

    private static final Logger LOG = Logger.getLogger(CreateLinkGraph.class);
    private static boolean titanAsDataSink = false;

    private static CommandLineInterface commandLineInterface = new CommandLineInterface();

    static {

        Options options = new Options();

        options.addOption(BaseCLI.Options.inputPath.get());

        options.addOption(BaseCLI.Options.outputPath.get());

        options.addOption(BaseCLI.Options.titanStorage.get());

        options.addOption(BaseCLI.Options.titanAppend.get());

        commandLineInterface.setOptions(options);
    }

    /*
     * This function checks whether required input path and output path
     * are specified as command line arguments
     *
     */
    private static void checkCli(CommandLine cmd) {
        String outputPath = null;

        String outputPathOpt = BaseCLI.Options.outputPath.getLongOpt();
        String titanStorageOpt = BaseCLI.Options.titanStorage.getLongOpt();

        if (cmd.hasOption(outputPathOpt) && cmd.hasOption(titanStorageOpt)) {
            commandLineInterface.showError("You cannot simultaneously specify a file and Titan for the output.");
        } else if (!cmd.hasOption(titanStorageOpt) && cmd.hasOption(BaseCLI.Options.titanAppend.getLongOpt())) {
            commandLineInterface.showError("You cannot append a Titan graph if you do not write to Titan. (Add the -t option if you meant to do this.)");
        } else if (cmd.hasOption(outputPathOpt)) {
            outputPath = cmd.getOptionValue(outputPathOpt);
            LOG.info("GRAPHBUILDER_INFO: output path: " + outputPath);
        } else if (cmd.hasOption(titanStorageOpt)) {
            titanAsDataSink = true;
        } else {
            commandLineInterface.showError("GRAPHBUILDER_ERROR: An output path is required");
        }
    }

    /**
     * This is the main method for creating the link graph.
     * @param args The raw command line.
     */

    public static void main(String[] args) {
        CommandLine cmd = commandLineInterface.checkCli(args);
        //check the parsed option against some custom logic
        checkCli(cmd);

        Timer timer = new Timer();

        GraphConstructionPipeline pipeline = new GraphConstructionPipeline();
        commandLineInterface.addConfig(pipeline);

        String inputPathName = commandLineInterface.getOptionValue("in");

        WikiPageInputFormat        format             = new WikiPageInputFormat();
        TextFileInputConfiguration inputConfiguration = new TextFileInputConfiguration(format, inputPathName);
        GraphBuildingRule          buildingRule       = new LinkGraphBuildingRule();

        OutputConfiguration outputConfiguration = null;

        if (titanAsDataSink) {
            outputConfiguration = new TitanOutputConfiguration();
        } else {
            String outputPathName = commandLineInterface.getOptionValue("o");
            outputConfiguration = new TextGraphOutputConfiguration(outputPathName);
        }

        LOG.info("========== Creating link graph ================");
        timer.start();
        pipeline.run(inputConfiguration, buildingRule,
                GraphConstructionPipeline.BiDirectionalHandling.KEEP_BIDIRECTIONALEDGES, outputConfiguration,
                commandLineInterface.getCmd());
        LOG.info("========== Done creating link graph ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}
