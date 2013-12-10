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

import com.intel.hadoop.graphbuilder.pipeline.input.text.TextFileInputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.input.text.textinputformats.WikiPageInputFormat;
import com.intel.hadoop.graphbuilder.pipeline.output.OutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.textgraph.TextGraphOutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanOutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.wordcountgraph.WordCountGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.GraphConstructionPipeline;
import com.intel.hadoop.graphbuilder.util.BaseCLI;
import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.hadoop.graphbuilder.util.Timer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

/**
 * Generates a word count graph from a collection of wiki pages.
 * <p>
 * The word count graph is a bipartite graph between wiki pages and words.
 * <ul>
 *     <li>There is a "contains" edge between every page and every word that it contains.</li>
 *     <li>The "contains" edge between a page and a word contains the frequency of the word.</li>
 * </ul>
 * </p>
 *
 * <p>At present there are two possible datasinks, a TextGraph, or a load into the Titan graph database. At present,
 * only one datasink can be specified for each run.
 * <ul>
 *     <li>To specify a text output, use the option: <code>-o directory_name </code></li>
 *     <li>To specify a Titan load, use the option: <code>-t</code>
 *     <ul><li>The tablename used by Titan is specified in the config file specified at: <code> -conf conf_path </code></li>
 *     <li>If no tablename is specified, Titan uses the default table name: <code>titan</code></li>
 *     <li>The <code>-a</code> option that tells Titan it can append the newly generated graph to an existing
 *         one in the same table. The default behavior is to abort if you try to use an existing Titan table name.</li> </ul>
 * </ul>
 * </p>
 *
 */
public class CreateWordCountGraph {

    private static final   Logger  LOG             = Logger.getLogger(CreateWordCountGraph.class);
    private static         boolean titanAsDataSink = false;

    private static CommandLineInterface commandLineInterface = new CommandLineInterface();
    static {
        Options options = new Options();

        options.addOption(BaseCLI.Options.inputPath.get());

        options.addOption(BaseCLI.Options.outputPath.get());

        options.addOption(BaseCLI.Options.titanStorage.get());

        options.addOption(BaseCLI.Options.titanAppend.get());

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

    /*
     * This function checks whether the required input path and output path
     * are specified as command line arguments.
     *
     */
    private static void checkCli(String[] args) {
        String outputPath = null;

        CommandLine cmd = commandLineInterface.parseArgs(args);

        String outputPathOpt = BaseCLI.Options.outputPath.getLongOpt();
        String titanStorageOpt = BaseCLI.Options.titanStorage.getLongOpt();

        if (cmd.hasOption(outputPathOpt) && cmd.hasOption(titanStorageOpt)) {
            commandLineInterface.showError("You cannot simultaneously specify a file and Titan for the output.");
        } else if (!cmd.hasOption(titanStorageOpt) && cmd.hasOption(BaseCLI.Options.titanAppend.getLongOpt())) {
            commandLineInterface.showError("You cannot append a Titan graph if you do not write to Titan. (Add the -t option if you meant to do this.)");
        } else if (cmd.hasOption(outputPathOpt)) {
            outputPath = cmd.getOptionValue(outputPathOpt);
            LOG.info("output path: " + outputPath);
        } else if (cmd.hasOption(titanStorageOpt)) {
            titanAsDataSink = true;
        } else {
            commandLineInterface.showError("An output path is required");
        }

    }

    /**
     * The main method for creating a wordcount graph.
     * @param args  The raw command line from the user.
     */

    public static void main(String[] args)  {
        CommandLine cmd = commandLineInterface.checkCli(args);
        //application specific argument checks
        checkCli(args);

        Timer timer = new Timer();

        GraphConstructionPipeline pipeline = new GraphConstructionPipeline();
        commandLineInterface.getRuntimeConfig().addConfig(pipeline);

        if (commandLineInterface.hasOption("d")) {
            String dictionaryPath = commandLineInterface.getOptionValue("dictionary");
            pipeline.addUserOpt("Dictionary", dictionaryPath);
            LOG.info("Dictionary path: " + dictionaryPath);
        }

        if (commandLineInterface.hasOption("s")) {
            String stopwordsPath = commandLineInterface.getOptionValue("stopwords");
            pipeline.addUserOpt("Dictionary", stopwordsPath);
            LOG.info("Stopwords path: " + stopwordsPath);
        }

        String                     inputPathName      = commandLineInterface.getOptionValue("in");
        TextInputFormat            format             = new WikiPageInputFormat();
        TextFileInputConfiguration inputConfiguration = new TextFileInputConfiguration(format, inputPathName);
        WordCountGraphBuildingRule graphBuildingRule  = new WordCountGraphBuildingRule();

        OutputConfiguration outputConfiguration = null;

        if (titanAsDataSink) {
            outputConfiguration = new TitanOutputConfiguration();
        }
        else {
            String outputPathName = commandLineInterface.getOptionValue("o");
            outputConfiguration = new TextGraphOutputConfiguration(outputPathName);
        }

        LOG.info("============= Creating Word Count Graph ===================");
        timer.start();
        pipeline.run(inputConfiguration, graphBuildingRule,
                GraphConstructionPipeline.BiDirectionalHandling.REMOVE_BIDIRECTIONALEDGES,
                outputConfiguration, commandLineInterface.getCmd());
        LOG.info("========== Done Creating Word Count Graph  ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}
