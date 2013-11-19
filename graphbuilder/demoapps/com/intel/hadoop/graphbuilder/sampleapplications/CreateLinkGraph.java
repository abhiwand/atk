/* Copyright (C) 2012 Intel Corporation.
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
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanCommandLineOptions;
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
 * Generate a link graph from a collection of wiki pages.
 * <p>
 * <ul>
 *     <li>There is vertex for every wiki page in the specified input file.</li>
 *     <li>There is a "linksTO" edge from each page to every page to which it links.</li>
 * </ul>
 * </p>
 *
 * <p>At present there are two possible datasinks, a TextGraph, or a load into the Titan graph database. At present,
 * only one datasink can be specified for each run.
 * <ul>
 *     <li>To specify a text output: use option <code>-o directory_name </code></li>
 *     <li>To specify a Titan load, use the option <code>-t</code>
 *     <ul><li>The tablename used by Titan is specified in the config file specified at <code> -conf conf_path </code></li>
 *     <li>If no tablename is specified, Titan uses the default table name <code>titan</code></li>
 *     <li><code>-a</code> an option that tells Titan it can append the newly generated graph to an existing
 *         one in the same table. Default behavior is to abort if you try to use an existing Titan table name</li></ul>
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

        options.addOption("t", "titan", false, "select Titan for graph storage");

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

        if (cmd.hasOption("out") && cmd.hasOption("titan")) {
            commandLineInterface.showError("You cannot simultaneously specify a file and Titan for the output.");
        } else if (!cmd.hasOption("titan") && cmd.hasOption(TitanCommandLineOptions.APPEND)) {
            commandLineInterface.showError("You cannot append a Titan graph if you do not write to Titan. (Add the -t option if you meant to do this.)");
        } else if (cmd.hasOption("out")) {
            outputPath = cmd.getOptionValue("out");
            LOG.info("output path: " + outputPath);
        } else if (cmd.hasOption("titan")) {
            titanAsDataSink = true;
        } else {
            commandLineInterface.showError("An output path is required");
        }
    }

    /**
     * Encapsulation of the job setup process.
     */
    public class ConstructionPipeline extends GraphConstructionPipeline {

        @Override
        public boolean shouldCleanBiDirectionalEdges() {
            return false;
        }

        @Override
        public boolean shouldUseHBase() {
            return false;
        }
    }

    /**
     * Main method for creating the link graph
     * @param args raw command line
     * @throws Exception
     */

    public static void main(String[] args) {
        CommandLine cmd = commandLineInterface.checkCli(args);
        //check the parsed option against some custom logic
        checkCli(cmd);

        Timer timer = new Timer();

        ConstructionPipeline job = new CreateLinkGraph().new ConstructionPipeline();
        job = (ConstructionPipeline) commandLineInterface.addConfig(job);

        WikiPageInputFormat        format             = new WikiPageInputFormat();
        TextFileInputConfiguration inputConfiguration = new TextFileInputConfiguration(format);
        GraphBuildingRule          buildingRule       = new LinkGraphBuildingRule();

        OutputConfiguration outputConfiguration = null;

        if (titanAsDataSink) {
            outputConfiguration = new TitanOutputConfiguration();
        } else {
            outputConfiguration = new TextGraphOutputConfiguration();
        }

        LOG.info("========== Creating link graph ================");
        timer.start();
        job.run(inputConfiguration, buildingRule, outputConfiguration, cmd);
        LOG.info("========== Done creating link graph ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}
