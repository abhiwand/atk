//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.giraph.io.formats;

import com.intel.giraph.io.GaussianDistWritable;
import com.intel.giraph.io.VertexData4GBPWritable;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class TestJsonPropertyGraph4GBPOutputFormat extends JsonPropertyGraph4GBPOutputFormat {
    /** Test configuration */
    private ImmutableClassesGiraphConfiguration<LongWritable, VertexData4GBPWritable, Writable> conf;

    /**
     * Dummy class to allow ImmutableClassesGiraphConfiguration to be created.
     */
    public static class DummyComputation extends NoOpComputation<LongWritable, VertexData4GBPWritable, Writable,
        Writable> { }

    @Before
    public void setUp() {
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
        giraphConfiguration.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<LongWritable, VertexData4GBPWritable,
            Writable>(giraphConfiguration);
    }

    @Test
    public void testOuputFormat() throws IOException, InterruptedException {
        Text expected = new Text("[1,4]");

        JsonPropertyGraph4GBPOutputFormatTestWorker(expected);
    }
  
    private void JsonPropertyGraph4GBPOutputFormatTestWorker(Text expected) throws IOException,
        InterruptedException {
        TaskAttemptContext tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);

        Vertex vertex = mock(Vertex.class);
        when(vertex.getId()).thenReturn(new LongWritable(1L));
        when(vertex.getValue()).thenReturn(new VertexData4GBPWritable(new GaussianDistWritable(2.0, 3.0),
            new GaussianDistWritable(4.0, 5.0), new GaussianDistWritable(2.0, 3.0), 2.0));

        // Create empty iterator == no edges
        when(vertex.getEdges()).thenReturn(new ArrayList<Text>());

        final RecordWriter<Text, Text> tw = mock(RecordWriter.class);

        JsonPropertyGraph4GBPWriter writer = new JsonPropertyGraph4GBPWriter() {
            @Override
            protected RecordWriter<Text, Text> createLineRecordWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
                return tw;
            }
        };
        writer.setConf(conf);
        writer.initialize(tac);
        writer.writeVertex(vertex);

        verify(tw).write(expected, null);
        verify(vertex, times(0)).getEdges();
    }

}