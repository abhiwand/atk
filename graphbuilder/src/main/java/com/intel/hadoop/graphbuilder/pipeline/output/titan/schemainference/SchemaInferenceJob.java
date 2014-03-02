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

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * javadoc todo
 *
 */
public class SchemaInferenceJob {
    private static final Logger LOG = Logger.getLogger(SchemaInferenceJob.class);


    public void run(Configuration conf, Path inputPath)
            throws IOException, InterruptedException, ClassNotFoundException {

    Job schemaInferenceJob =  new Job(conf, "Inferring Graph Schema for Write to Titan");

    schemaInferenceJob.setJarByClass(SchemaInferenceJob.class);

    // configure mapper  and input

    schemaInferenceJob.setMapperClass(SchemaInferenceMapper.class);
    schemaInferenceJob.setCombinerClass(SchemaInferenceCombiner.class);
    schemaInferenceJob.setReducerClass(SchemaInferenceReducer.class);

    schemaInferenceJob.setMapOutputKeyClass(NullWritable.class);
    schemaInferenceJob.setMapOutputValueClass(EdgeOrPropertySchema.class);

    try {
        FileInputFormat.addInputPath(schemaInferenceJob,
                inputPath);
    } catch (IOException e) {
        GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                .UNHANDLED_IO_EXCEPTION,
                "GRAPHBUILDER_ERROR: Cannot access temporary edge file.",
                LOG, e);
    }




    // the output only goes to Titan, not to HDFS;

    schemaInferenceJob.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
    schemaInferenceJob.setOutputKeyClass(NullWritable.class);
    schemaInferenceJob.setOutputValueClass(NullWritable.class);


    LOG.info("=========== Job 2: Propagate Titan Vertex IDs to Edges " +
            "  ===========");

    LOG.info("==================== Start " +
            "====================================");
    schemaInferenceJob.waitForCompletion(true);
    LOG.info("=================== Done " +
            "====================================\n");
    }
}
