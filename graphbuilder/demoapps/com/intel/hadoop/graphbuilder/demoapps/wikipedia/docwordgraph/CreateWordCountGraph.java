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

package com.intel.hadoop.graphbuilder.demoapps.wikipedia.docwordgraph;

import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.TextGraphOutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.OutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.TitanOutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.TextFileInputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.job.AbstractCreateGraphJob;

import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.hadoop.graphbuilder.util.Timer;
import org.apache.commons.cli.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.inputformat.WikiPageInputFormat;

public class CreateWordCountGraph {

    private static final   Logger  LOG             = Logger.getLogger(CreateWordCountGraph.class);
    private static         boolean titanAsDataSink = false;

    public class Job extends AbstractCreateGraphJob {
        @Override
        public boolean cleanBidirectionalEdge() {
            return true;
        }

        @Override
        public boolean usesHBase() {
            return false;
        }
    }

    private static CommandLineInterface commandLineInterface = new CommandLineInterface();
    static {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("in")
                .withDescription("input path")
                .hasArgs()
                .isRequired()
                .withArgName("input path")
                .create("i"));
        options.addOption(OptionBuilder.withLongOpt("out")
                .withDescription("output path")
                .hasArgs()
                .withArgName("output path")
                .create("o"));
        options.addOption(OptionBuilder.withLongOpt("titan")
                .withDescription("select Titan for graph storage")
                .withArgName("titan ")
                .create("t"));
        options.addOption(OptionBuilder.withLongOpt("dictionary")
                .withDescription("dictionary path")
                .hasArgs()
                .withArgName("dictionary path")
                .create("d"));
        options.addOption(OptionBuilder.withLongOpt("stopwords")
                .withDescription("stop words path")
                .hasArgs()
                .withArgName("stop words path")
                .create("s"));
        commandLineInterface.setOptions(options);
    }

    /**
     * This function checks whether required input path and output path
     * are specified as command line arguments
     *
     * @param args Command line parameters
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
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        commandLineInterface.checkCli(args);
        checkCli(args);

        Timer timer = new Timer();

        Job job = new CreateWordCountGraph().new Job();
        job = (Job) commandLineInterface.getRuntimeConfig().addConfig(job);

        if (commandLineInterface.hasOption("d")) {
            String dictionaryPath = commandLineInterface.getOptionValue("dictionary");
            job.addUserOpt("Dictionary", dictionaryPath);
            LOG.info("Dictionary path: " + dictionaryPath);
        }

        if (commandLineInterface.hasOption("s")) {
            String stopwordsPath = commandLineInterface.getOptionValue("stopwords");
            job.addUserOpt("Dictionary", stopwordsPath);
            LOG.info("Stopwords path: " + stopwordsPath);
        }

        TextInputFormat            format             = new WikiPageInputFormat();
        TextFileInputConfiguration inputConfiguration = new TextFileInputConfiguration(format);
        WordCountGraphBuildingRule graphBuildingRule  = new WordCountGraphBuildingRule();

        OutputConfiguration outputConfiguration = null;

        if (titanAsDataSink) {
            outputConfiguration = new TitanOutputConfiguration();
        }
        else {
            outputConfiguration = new TextGraphOutputConfiguration();
        }

        LOG.info("============= Creating Word Count Graph ===================");
        timer.start();
        job.run(inputConfiguration, graphBuildingRule, outputConfiguration, commandLineInterface.getCmd());
        LOG.info("========== Done Creating Word Count Graph  ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}
