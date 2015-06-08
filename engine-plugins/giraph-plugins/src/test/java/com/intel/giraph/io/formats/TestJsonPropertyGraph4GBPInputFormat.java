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

import com.intel.giraph.io.VertexData4GBPWritable;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.NoOpComputation;
import com.intel.giraph.io.EdgeData4GBPWritable;
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
    private ImmutableClassesGiraphConfiguration<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> conf;
    private TaskAttemptContext tac;

    @Before
    public void setUp() throws IOException, InterruptedException {
        rr = mock(RecordReader.class);
        when(rr.nextKeyValue()).thenReturn(true);
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<LongWritable, VertexData4GBPWritable,
            EdgeData4GBPWritable>(giraphConf);
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
        Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex = vr.getCurrentVertex();
        assertEquals(vertex.getNumEdges(), 3);
        assertEquals(1L, vertex.getId().get());
        assertEquals(0.2, vertex.getValue().getPrior().getMean(), 0d);
        assertEquals(2.0, vertex.getValue().getPrior().getPrecision(), 0d);
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(0L)).getWeight(), 0d);
        assertEquals(2.0, vertex.getEdgeValue(new LongWritable(2L)).getWeight(), 0d);
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(3L)).getWeight(), 0d);
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(0L)).getReverseWeight(), 0d);
        assertEquals(2.0, vertex.getEdgeValue(new LongWritable(2L)).getReverseWeight(), 0d);
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(3L)).getReverseWeight(), 0d);
    }

    public static class DummyComputation extends NoOpComputation<LongWritable, VertexData4GBPWritable,
        EdgeData4GBPWritable, Writable> { }

}
