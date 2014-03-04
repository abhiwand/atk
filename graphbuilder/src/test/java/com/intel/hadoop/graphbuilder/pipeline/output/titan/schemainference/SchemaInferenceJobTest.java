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

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SerializedEdgeOrPropertySchema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Job.class, FileInputFormat.class})
public class SchemaInferenceJobTest {

    @Mock
    Path mockedInputPath;

    @Mock
    Job mockedJob;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRun() throws Exception {

        SchemaInferenceJob schemaInferenceJob = new SchemaInferenceJob();

        mockStatic(FileInputFormat.class);

        schemaInferenceJob.setPath(mockedInputPath);
        schemaInferenceJob.setJob(mockedJob);

        schemaInferenceJob.run();
        verify(mockedJob).setMapperClass(SchemaInferenceMapper.class);
        verify(mockedJob).setCombinerClass(SchemaInferenceCombiner.class);
        verify(mockedJob).setReducerClass(SchemaInferenceReducer.class);
        verify(mockedJob).setMapOutputKeyClass(NullWritable.class);

        verify(mockedJob).setMapOutputValueClass(SerializedEdgeOrPropertySchema.class);

        verify(mockedJob).setInputFormatClass(SequenceFileInputFormat.class);

        verify(mockedJob).setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
        verify(mockedJob).setOutputKeyClass(NullWritable.class);
        verify(mockedJob).setOutputValueClass(SerializedEdgeOrPropertySchema.class);
    }
}
