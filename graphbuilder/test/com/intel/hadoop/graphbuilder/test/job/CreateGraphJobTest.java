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
package com.intel.hadoop.graphbuilder.test.job;

import com.intel.hadoop.graphbuilder.demoapps.wikipedia.linkgraph.LinkGraphBuildingRule;
import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.TextGraphOutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.TextFileInputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.job.AbstractCreateGraphJob;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.inputformat.WikiPageInputFormat;
import com.intel.hadoop.graphbuilder.demoapps.wikipedia.linkgraph.LinkGraphTokenizer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Test runnable for creating a link graph from wikipedia xml file.
 * This test simply tests that the application can execute without error.
 * It does not test correctness of the output.
 */

public class CreateGraphJobTest {

    public class Job extends AbstractCreateGraphJob {

        @Override
        public boolean shouldCleanBiDirectionalEdges() {
            return false;
        }

        @Override
        public boolean shouldUseHBase() {
            return false;
        }
    }

    private static Options createOptions() {

        Options options = new Options();
        options.addOption("i", "in", true, "input path");
        options.addOption("o", "out", true, "output path");
        return options;
    }

    /**
     * @param args [inputPath, outputPath]
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {

        CreateGraphJobTest test = new CreateGraphJobTest();

        Job               job     = test.new Job();
        CommandLineParser parser  = new PosixParser();
        Options           options = createOptions();
        CommandLine       cmd     = parser.parse(options, args);

        if (null == cmd) {
            System.exit(1);
        }

        TextInputFormat            format             = new WikiPageInputFormat();
        TextFileInputConfiguration inputConfiguration = new TextFileInputConfiguration(format);
        LinkGraphBuildingRule      graphBuildingRule  = new LinkGraphBuildingRule();

        TextGraphOutputConfiguration outputConfiguration = new TextGraphOutputConfiguration();

        job.run(inputConfiguration, graphBuildingRule, outputConfiguration, cmd);
    }
}
