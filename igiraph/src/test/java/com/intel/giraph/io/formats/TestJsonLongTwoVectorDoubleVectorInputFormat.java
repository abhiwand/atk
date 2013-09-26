package com.intel.giraph.io.formats;

import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import com.intel.giraph.io.formats.JsonLongTwoVectorDoubleVectorInputFormat;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.intel.mahout.math.TwoVectorWritable;
import com.intel.mahout.math.DoubleWithVectorWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJsonLongTwoVectorDoubleVectorInputFormat extends JsonLongTwoVectorDoubleVectorInputFormat {

    private RecordReader<LongWritable, Text> rr;
    private ImmutableClassesGiraphConfiguration<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> conf;
    private TaskAttemptContext tac;

    @Before
    public void setUp() throws IOException, InterruptedException {
        rr = mock(RecordReader.class);
        when(rr.nextKeyValue()).thenReturn(true);
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<LongWritable, TwoVectorWritable,
            DoubleWithVectorWritable>(giraphConf);
        tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);
    }

    protected TextVertexReader createVertexReader(final RecordReader<LongWritable, Text> rr) {
        return new JsonLongTwoVectorDoubleVectorReader() {
        @Override
        protected RecordReader<LongWritable, Text> createLineRecordReader(InputSplit inputSplit,
            TaskAttemptContext context) throws IOException, InterruptedException {
            return rr;
        }
        };
    }

    @Test
    public void testReadVertex() throws Exception {
        String input = "[1,[0.2,2,2],[[0,1],[2,2],[3,1]]]";

        when(rr.getCurrentValue()).thenReturn(new Text(input));
        TextVertexReader vr = createVertexReader(rr);
        vr.setConf(conf);
        vr.initialize(null, tac);

        assertTrue("Should have been able to read vertex", vr.nextVertex());
        Vertex<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> vertex = vr.getCurrentVertex();
        assertEquals(vertex.getNumEdges(), 3);
        assertEquals(1L, vertex.getId().get());
        assertEquals(3, vertex.getValue().getPriorVector().size());
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(0L)).getData(), 0d);
        assertEquals(2.0, vertex.getEdgeValue(new LongWritable(2L)).getData(), 0d);
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(3L)).getData(), 0d);
    }

    public static class DummyComputation extends NoOpComputation<LongWritable, TwoVectorWritable,
        DoubleWithVectorWritable, Writable> { }

}
