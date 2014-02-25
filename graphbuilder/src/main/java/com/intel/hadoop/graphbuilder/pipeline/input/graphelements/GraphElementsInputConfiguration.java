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
package com.intel.hadoop.graphbuilder.pipeline.input.graphelements;

import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This class handles the set-up time configuration when the input is a sequence file with
 * {@code NullWritable} keys and {@code SerializedGraphElement} values.
 * <p/>
 * For graph construction tasks that require multiple chained Map Reduce jobs, this class
 * affects only the first Map Reduce job, as that is the first mapper that deals with raw input.
 * <p/>
 * <ul>
 * <li> It provides a handle to the mapper class used to read sequence files ({@code GraphElementsReaderMapper}).</li>
 * <li> It provides the input path name to the Hadoop job.</li>
 * </ul>
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration
 * @see com.intel.hadoop.graphbuilder.pipeline.input.graphelements.GraphElementsReaderMapper
 */
public class GraphElementsInputConfiguration implements InputConfiguration {

    private static final Logger LOG = Logger.getLogger(GraphElementsInputConfiguration.class);

    private Path inputPath;

    private static final Class mapperClass = GraphElementsReaderMapper.class;
    private static final Class inputFormat = SequenceFileInputFormat.class;

    /**
     * The constructor takes the input path for the sequence file of graph elements.
     *
     * @param inputPathString path to the sequence file of graph elements.
     */
    public GraphElementsInputConfiguration(String inputPathString) {
        this.inputPath = new Path(inputPathString);
    }

    /**
     * This input configuration does not use hbase.
     *
     * @return {@literal false }
     */
    public boolean usesHBase() {
        return false;
    }

    /**
     * A no-op.
     */

    public void updateConfigurationForMapper(Configuration configuration) {

    }

    /**
     * Sets the mapper class and input path for the Hadoop job.
     */
    public void updateJobForMapper(Job job) {
        job.setMapperClass(mapperClass);

        job.setInputFormatClass(inputFormat);

        try {
            FileInputFormat.addInputPath(job, inputPath);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_LOAD_INPUT_FILE,
                    "Unable to set input path " + inputPath, LOG, e);
        }
    }

    /**
     * Returns the mapper class.
     */
    public Class getMapperClass() {
        return mapperClass;
    }

    /**
     * Obtains a description of the input configuration for logging purposes.
     *
     * @return "Sequence file of graph elements at: " Appended with the source input path.
     */
    public String getDescription() {
        return "Sequence file of graph elements at " + inputPath.toString();
    }
}
