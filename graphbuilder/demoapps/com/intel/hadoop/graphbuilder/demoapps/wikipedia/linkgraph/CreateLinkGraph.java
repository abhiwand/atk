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
package com.intel.hadoop.graphbuilder.demoapps.wikipedia.linkgraph;

import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.TextGraphOutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.OutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.TitanOutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.TextFileInputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.inputformat.WikiPageInputFormat;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.job.AbstractCreateGraphJob;
import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.hadoop.graphbuilder.util.Timer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
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
 *     <li>If you try to write to an existing table, Titan will append the graph to the existing table</li></ul>
 *     </li>
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

        options.addOption(OptionBuilder.withLongOpt("in")
                .withDescription("input path")
                .hasArgs()
                .isRequired()
                .withArgName("input path")
                .create("i"));
        //options.addOption("o", "out",   true,  "output path");
        options.addOption(OptionBuilder.withLongOpt("out")
                .withDescription("output path")
                .hasArgs()
                .withArgName("output path")
                .create("o"));
        options.addOption("t", "titan", false, "select Titan for graph storage");

        commandLineInterface.setOptions(options);
    }

    /*
     * This function checks whether required input path and output path
     * are specified as command line arguments
     *
     */
    private static void checkCli(String[] args) {
        String outputPath = null;

        CommandLine cmd = commandLineInterface.parseArgs(args);

        if (cmd.hasOption("out") && cmd.hasOption("titan")) {
            commandLineInterface.showHelp("You cannot simultaneously specify a file and Titan for the output.");
        } else if (cmd.hasOption("out")) {
            outputPath = cmd.getOptionValue("out");
            LOG.info("output path: " + outputPath);
        } else if (cmd.hasOption("titan")) {
            titanAsDataSink = true;
        } else {
            commandLineInterface.showHelp("An output path is required");
        }
    }

    /**
     * Encapsulation of the job setup process.
     */
    public class Job extends AbstractCreateGraphJob {

        @Override
        public boolean cleanBidirectionalEdge() {
            return false;
        }

        @Override
        public boolean usesHBase() {
            return false;
        }
    }

    /**
     * Main method for creating the link graph
     * @param args raw command line
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        commandLineInterface.checkCli(args);
        checkCli(args);

        Timer timer = new Timer();

        if (null == commandLineInterface.getCmd()) {
            commandLineInterface.showHelp("Error parsing command line options");
        }

        Job job = new CreateLinkGraph().new Job();
        job = (Job) commandLineInterface.getRuntimeConfig().addConfig(job);

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
        job.run(inputConfiguration, buildingRule, outputConfiguration, commandLineInterface.getCmd());
        LOG.info("========== Done creating link graph ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}
