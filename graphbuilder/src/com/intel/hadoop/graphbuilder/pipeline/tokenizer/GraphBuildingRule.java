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

package com.intel.hadoop.graphbuilder.pipeline.tokenizer;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;

/**
 * Classes that implement this interface encapsulate the set-up time information necessary to convert records into
 * graph elements.
 * <p>
 *     Its primary responsibilities are to communicate the MR time graph tokenizer method and associated metadata
 *     (such as the schema of any graph generated by this method) to the other components at set-up time, as well as
 *     pass any required state to the job configuration for use by the tokenizer at MR time.
 * </p>
 *
 * <p>
 *     The MR-time analog  of this class is the @code GraphTokenizer interface
 * </p>
 *
 * @see GraphTokenizer
 *
 */
public interface GraphBuildingRule {

    /**
     * Take user-specified information from command line parameters, extracts state for controlling the
     * graph construction process and packs it into the job configuration for use by the graph tokenizer
     * during MR time.
     *
     * @param configuration  reference to the job configuration in which params for tokenizer will be stored
     * @param cmd  the command line options provided by the use
     */

    public void    updateConfigurationForTokenizer (Configuration configuration, CommandLine cmd);

    /**
     * Obtain the type information for the graphs this method can generate.
     *
     * The graph construction (tokenization) process determines all of the vertex, edge and property types that
     * can appear in the resulting property graph.  This type information is sometimes needed by the graph storage
     * target.
     *
     * @return the property graph schema for the graph for the graph elements generated
     */

    public PropertyGraphSchema getGraphSchema();

    /**
     * Get the class for the MR time graph generation method, the graph tokenizer
     * @return   Class<? extends GraphTokenizer>   Class of the MR-time graph tokenizer used by this graph building rule.
     * @see GraphTokenizer
     */

    public Class<? extends GraphTokenizer> getGraphTokenizerClass();

    /**
     * Get the vertex ID type. Used for type safety at set-up time.
     * @return Class of the vertex ID type.
     */
    public Class vidClass();
}
