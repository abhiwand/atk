package com.intel.hadoop.graphbuilder.sampleapplications;

/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */

import com.intel.hadoop.graphbuilder.pipeline.GraphConstructionPipeline;
import com.intel.hadoop.graphbuilder.pipeline.input.graphelements.GraphElementsInputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanCommandLineOptions;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanOutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.passthrough.PassThroughGraphBuildingRule;
import com.intel.hadoop.graphbuilder.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Take a sequence file of serialized graph elements and load into a graph database.
 * <p>
 * At present, we support only Titan for the graph database.
 * </p>
 * <p/>
 * <p>
 * Arguments relating to input/output control:
 * <ul>
 * <li>The <code>-i</code> option specifies the path of the sequence file to be read. (Required.) </li>
 * <li>The <code>-conf</code> option specifies Titan configuration file. (Required.)</li>
 * <li>The <code>-a</code> option tells Titan it can append the newly generated graph to an existing
 * one in the same table. The default behavior is to abort if you try to use an existing Titan table name.</li>
 * <li>The <code>-O</code> option tells Titan to throw out the old graph and overwrite with the incoming. </li>
 * </ul>
 * Specify the Titan table name in the configuration file in the property:
 * <code>graphbuilder.titan.storage_tablename</code>
 * </p>
 * <p/>
 * <p>
 * Arguments relating to type declaration:
 * <ul>
 * <li>The <code>-p</code> required argument specifies the datatypes for the property names.
 * It is a comma separated list of the form
 * {@code property1:datatype1,property2:datatype2,....} </li>
 * <li>The <code>-E</code> required argument specifies the signatures of the edge labels.
 * It is a semi-colon separated list of comma separated lists. Each comma separated
 * list specifies an edge label followed by the property names that can be associated with
 * an edge of that label. Eg.
 * {@code label1, property11, property12;label2;label3,property31;....}</li>
 * </ul>
 * </p>
 */

public class GraphElementsToDB {

    private static final Logger LOG = Logger.getLogger(GraphElementsToDB.class);

    private static CommandLineInterface commandLineInterface = new CommandLineInterface();

    static {
        Options options = new Options();

        options.addOption(BaseCLI.Options.inputPath.get());

        options.addOption(BaseCLI.Options.titanAppend.get());

        options.addOption(BaseCLI.Options.titanOverwrite.get());

        options.addOption(BaseCLI.Options.titanPropertyTypes.get());

        options.addOption(BaseCLI.Options.titanEdgeSignatures.get());

        options.addOption(BaseCLI.Options.titanKeyIndex.get());

        options.addOption(BaseCLI.Options.titanInferSchema.get());

        commandLineInterface.setOptions(options);
    }

    /*
     * Private helper to extract property name to datatype classname from incoming args
     */
    private static HashMap<String, Class<?>> extractPropertyTypeMap(String propNamesString) {
        HashMap<String, Class<?>> map = new HashMap<String, Class<?>>();

        if (!propNamesString.isEmpty()) {

            String[] nameTypePairs = propNamesString.split(",");

            for (String nameTypePair : nameTypePairs) {
                String[] separated = nameTypePair.split(":");

                if (separated.length >= 2) {
                    String name = separated[0];
                    String typeName = separated[1];

                    Class<?> dataType = null;

                    if (typeName.equals(TitanCommandLineOptions.STRING_DATATYPE)) {
                        dataType = String.class;
                    } else if (typeName.equals
                            (TitanCommandLineOptions.INT_DATATYPE)) {
                        dataType = Integer.class;
                    } else if (typeName.equals
                            (TitanCommandLineOptions.LONG_DATATYPE)) {
                        dataType = Long.class;
                    } else if (typeName.equals
                            (TitanCommandLineOptions.DOUBLE_DATATYPE)) {
                        dataType = Double.class;
                    } else if (typeName.equals
                            (TitanCommandLineOptions.FLOAT_DATATYPE)) {
                        dataType = Float.class;
                    } else {
                        GraphBuilderExit.graphbuilderFatalExitNoException
                                (StatusCode.BAD_COMMAND_LINE,
                                        "Error cannot parse datatype declaration: " + nameTypePair + "\n" +
                                                TitanCommandLineOptions.KEY_DECLARATION_CLI_HELP, LOG);
                    }
                    map.put(name, dataType);
                }
            }
        }
        return map;
    }

    /*
     * helper for extracting the edge signatures as provided by pig script
     */
    private static HashMap<String, ArrayList<String>> extractEdgeSignatures(String edgeSignatureString) {

        HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();

        if (!edgeSignatureString.isEmpty()) {

            String[] edgeSignatures = edgeSignatureString.split(";");

            for (String edgeSignature : edgeSignatures) {

                String[] splitSignature = edgeSignature.split(",");

                String edgeLabel = splitSignature[0];
                ArrayList<String> properties = new ArrayList<String>();

                if (splitSignature.length > 1) {
                    for (int i = 1; i < splitSignature.length; i++) {
                        properties.add(splitSignature[i]);
                    }
                }

                map.put(edgeLabel, properties);
            }
        }

        return map;
    }

    /**
     * The main method for loading a sequence file of graph elements into a graph database.
     *
     * @param args Command line arguments.
     */

    public static void main(String[] args) {

        Timer timer = new Timer();

        boolean configFilePresent = (args.length > 0 && args[0].equals("-conf"));

        if (!configFilePresent) {
            commandLineInterface.showError("When writing to Titan, the Titan config file must be specified by -conf <config> ");
        }

        CommandLine cmd = commandLineInterface.checkCli(args);

        HashMap<String, Class<?>> propTypeMap = extractPropertyTypeMap(cmd.getOptionValue(
                BaseCLI.Options.titanPropertyTypes.getLongOpt()));

        HashMap<String, ArrayList<String>> edgeSignatures = extractEdgeSignatures(cmd.getOptionValue(
                BaseCLI.Options.titanEdgeSignatures.getLongOpt()));

        String inputPath = cmd.getOptionValue(BaseCLI.Options.inputPath.getLongOpt());

        GraphConstructionPipeline pipeline = new GraphConstructionPipeline();
        commandLineInterface.getRuntimeConfig().addConfig(pipeline);

        // the input configuration is now a sequence file configuration.
        GraphElementsInputConfiguration inputConfiguration = new GraphElementsInputConfiguration(inputPath);

        PassThroughGraphBuildingRule buildingRule = new PassThroughGraphBuildingRule(propTypeMap, edgeSignatures);

        // Titan output configuration is about the same
        // BUT... we will need an option to turn off the dedup phase...
        // it doesn't hurt us... it's just a waste of effort...

        TitanOutputConfiguration outputConfiguration = new TitanOutputConfiguration();

        LOG.info("============= Creating graph from feature table ==================");
        timer.start();
        pipeline.run(inputConfiguration, buildingRule,
                GraphConstructionPipeline.BiDirectionalHandling.KEEP_BIDIRECTIONALEDGES,
                outputConfiguration, cmd);
        LOG.info("========== Done creating graph from feature table ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}