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
package com.intel.hadoop.graphbuilder.job;

import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.OutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.InputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;

/**
 * An abstract class that connects the input configuration, tokenizer and output configuration
 * to generate a graph stored in the raw form as specified by the input configuration, using
 * the rules of the tokenizer and outputting the graph per the output configuration.
 *
 * It is here that the input and tokenizer are "hooked up into the" map reduce job (chain)
 * required by the output configuration.
 *
 *
 * @param <VidType>
 *
 * @see InputConfiguration
 * @see GraphTokenizer
 * @see OutputConfiguration
 *
 */

public abstract class AbstractCreateGraphJob<VidType extends WritableComparable<VidType>> {

    private HashMap<String, String> userOpts;

    public AbstractCreateGraphJob() {
        this.userOpts = new HashMap<String, String>();
    }

    public abstract boolean cleanBidirectionalEdge();

    public abstract boolean usesHBase();

    public void addUserOpt(String key, String value) {
        userOpts.put(key, value);
    }

    public boolean run(InputConfiguration  inputConfiguration,
                       GraphTokenizer      tokenizer,
                       OutputConfiguration outputConfiguration,
                       CommandLine         cmd)
            throws Exception {


        GraphGenerationMRJob graphGenerationMRJob = outputConfiguration.getGraphGenerationMRJob();

        // "hook up" the input configuration and tokenizer to the MR Job specified by the output configuration

        graphGenerationMRJob.init(inputConfiguration, tokenizer);

        Class vidClass   = tokenizer.vidClass();
        Class valueClass = ValueClassFactory.getValueClassByVidClassName(vidClass.getName());

        graphGenerationMRJob.setVidClass(vidClass);
        graphGenerationMRJob.setValueClass(valueClass);

        // Set optional parameters

        graphGenerationMRJob.setCleanBidirectionalEdges(cleanBidirectionalEdge());

        // Set user defined parameters

        if (userOpts != null) {
            graphGenerationMRJob.setUserOptions(userOpts);
        }

        try {
            graphGenerationMRJob.run(cmd);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
}
