/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
