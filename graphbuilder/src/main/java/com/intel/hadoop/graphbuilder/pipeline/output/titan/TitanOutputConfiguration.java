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
package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.pipeline.output.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.pipeline.output.OutputConfiguration;
import org.apache.hadoop.fs.Path;

/**
 * Encapsulates all state needed to write a property graph into Titan.
 */

public class TitanOutputConfiguration implements OutputConfiguration {
    private GraphGenerationMRJob graphGenerationMRJob;

    /**
     * No parameter constructor that allocates a pipeline for writing.
     * Schema will not be inferred dynamically, and the input path will be obtained from the
     * <code>InputConfiguration</code>
     */
    public TitanOutputConfiguration() {
        graphGenerationMRJob = new TitanWriterMRChain();
    }

    /**
     * Constructor that takes a boolean for whether or not to dynamically infer the graph schema with a map-reduce
     * scan over the data prior to initializing Titan.
     *
     * @param inferSchema <code>true</code>  means obtain the graph schema from a scan over the data,<code>false</code>
     *                    means it was obtained from the commandline.
     * @param pathName    The input path for a <code>SerializedGraphElementStringTypeVids</code> sequence file on HDFS.
     */
    public TitanOutputConfiguration(boolean inferSchema, String pathName) {
        Path path = new Path(pathName);
        graphGenerationMRJob = new TitanWriterMRChain(inferSchema, path);
    }

    /**
     * @return This output configuration's <code>GraphGenerationMRJob</code>;
     */
    public GraphGenerationMRJob getGraphGenerationMRJob() {
        return graphGenerationMRJob;
    }
}
