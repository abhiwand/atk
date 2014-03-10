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
package com.intel.hadoop.graphbuilder.pipeline.output.titan.schemainference;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SchemaElement;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Sets up and runs the schema inference job.
 * <p>This job performs a scan over all of the input, determines all of the properties, datatypes and edge labels that
 * appear in the data, and uses this information to configure Titan before loading.</p>
 */
public class SchemaInferenceJob {
    private static final Logger LOG = Logger.getLogger(SchemaInferenceJob.class);

    private Job job = null;
    private Path inputPath = null;

    /**
     * Constructor.
     *
     * @param conf      The Hadoop configuration for this job.
     * @param inputPath The path to the input file on HDFS.
     */
    public SchemaInferenceJob(Configuration conf, Path inputPath) {
        try {
            this.job = new Job(conf, "Inferring Graph Schema for Write to Titan");
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "Could not create new schema inference job.", LOG, e);
        }

        this.inputPath = inputPath;
    }

    /**
     * No argument constructor. Invoking <code>run</code> without setting both <code>setJOb</code> and <code>setPath</code> will
     * cause the job to fail.  This constructor was added for testing.
     */
    protected SchemaInferenceJob() {
    }

    /**
     * Sets the Hadoop Job object for this task.
     *
     * @param job A Hadoop <code>Job</code> Job object.
     */
    protected void setJob(Job job) {
        this.job = job;
    }

    /**
     * Sets the path to the input file on HDFS.
     *
     * @param path A path to a file on HDFS. It must be a null-keyed file of <code>SerializedGraphElement</code>'s.
     */
    protected void setPath(Path path) {
        this.inputPath = path;
    }

    /**
     * Runs the schema inference job.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public void run()
            throws IOException, InterruptedException, ClassNotFoundException {

        job.setJarByClass(SchemaInferenceJob.class);

        // configure mapper  and input

        job.setMapperClass(SchemaInferenceMapper.class);
        job.setCombinerClass(SchemaInferenceCombiner.class);
        job.setReducerClass(SchemaInferenceReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(SchemaElement.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        try {
            FileInputFormat.addInputPath(job, inputPath);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "GRAPHBUILDER_ERROR: Cannot access temporary edge file.", LOG, e);
        }

        // the output only goes to Titan, not to HDFS;

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SchemaElement.class);

        job.setNumReduceTasks(1);

        LOG.info("=========== Inferring Graph Schema ===========");

        job.waitForCompletion(true);
        LOG.info("=================== Done Inferring Graph Schema " +
                "====================================\n");
    }
}
