//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

import com.intel.giraph.io.VertexData4GBPWritable;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJsonPropertyGraph4GBPInputFormat extends JsonPropertyGraph4GBPInputFormat {

    private RecordReader<LongWritable, Text> rr;
    private ImmutableClassesGiraphConfiguration<LongWritable, VertexData4GBPWritable, DoubleWritable> conf;
    private TaskAttemptContext tac;

    @Before
    public void setUp() throws IOException, InterruptedException {
        rr = mock(RecordReader.class);
        when(rr.nextKeyValue()).thenReturn(true);
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<LongWritable, VertexData4GBPWritable,
            DoubleWritable>(giraphConf);
        tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);
    }

    protected TextVertexReader createVertexReader(final RecordReader<LongWritable, Text> rr) {
        return new JsonPropertyGraph4GBPReader() {
            @Override
            protected RecordReader<LongWritable, Text> createLineRecordReader(InputSplit inputSplit,
                TaskAttemptContext context) throws IOException, InterruptedException {
                    return rr;
                }
        };
    }

    @Test
    public void testReadVertex() throws Exception {
        String input = "[1,[0.2,2],[[0,1],[2,2],[3,1]]]";

        when(rr.getCurrentValue()).thenReturn(new Text(input));
        TextVertexReader vr = createVertexReader(rr);
        vr.setConf(conf);
        vr.initialize(null, tac);

        assertTrue("Should have been able to read vertex", vr.nextVertex());
        Vertex<LongWritable, VertexData4GBPWritable, DoubleWritable> vertex = vr.getCurrentVertex();
        assertEquals(vertex.getNumEdges(), 3);
        assertEquals(1L, vertex.getId().get());
        assertEquals(0.2, vertex.getValue().getPrior().getMean(), 0d);
        assertEquals(2.0, vertex.getValue().getPrior().getPrecision(), 0d);
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(0L)).get(), 0d);
        assertEquals(2.0, vertex.getEdgeValue(new LongWritable(2L)).get(), 0d);
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(3L)).get(), 0d);
    }

    public static class DummyComputation extends NoOpComputation<LongWritable, VertexData4GBPWritable,
        DoubleWritable, Writable> { }

}
