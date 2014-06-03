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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.LongWritable;
import com.intel.giraph.io.EdgeData4GBPWritable;
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

public class TestJsonGBPEdgeInputFormat extends JsonGBPEdgeInputFormat {

    private RecordReader<LongWritable, Text> rr;
    private ImmutableClassesGiraphConfiguration<LongWritable, Writable, EdgeData4GBPWritable> conf;
    private TaskAttemptContext tac;

    @Before
    public void setUp() throws IOException, InterruptedException {
        rr = mock(RecordReader.class);
        when(rr.nextKeyValue()).thenReturn(true);
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<LongWritable, Writable,
            EdgeData4GBPWritable>(giraphConf);
        tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);
    }

    protected TextEdgeReader createEdgeReader(final RecordReader<LongWritable, Text> rr) {
        return new JsonGBPEdgeReader() {
            @Override
            protected RecordReader<LongWritable, Text> createLineRecordReader(InputSplit inputSplit,
                TaskAttemptContext context) throws IOException, InterruptedException {
                    return rr;
                }
        };
    }

    @Test
    public void testReadEdge() throws Exception {
        String input = "[1,2,-2]";

        when(rr.getCurrentValue()).thenReturn(new Text(input));
        TextEdgeReader er = createEdgeReader(rr);
        er.setConf(conf);
        er.initialize(null, tac);

        assertTrue("Should have been able to read edge", er.nextEdge());
        assertEquals(1L, er.getCurrentSourceId().get());
        Edge<LongWritable, EdgeData4GBPWritable> edge = er.getCurrentEdge();
        assertEquals(2L, edge.getTargetVertexId().get());
        assertEquals(-2.0, edge.getValue().getWeight(), 0d);
        assertEquals(-2.0, edge.getValue().getReverseWeight(), 0d);
    }

    public static class DummyComputation extends NoOpComputation<LongWritable, Writable,
        EdgeData4GBPWritable, Writable> { }

}
