package com.intel.giraph.io.formats;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.DenseVector;
import com.intel.mahout.math.TwoVectorWritable;
import com.intel.giraph.io.formats.JsonLongIDVectorValueOutputFormat;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class TestJsonLongIDVectorValueOutputFormat extends JsonLongIDVectorValueOutputFormat {
    /** Test configuration */
    private ImmutableClassesGiraphConfiguration<LongWritable,
    TwoVectorWritable, Writable> conf;
    /**
     * Dummy class to allow ImmutableClassesGiraphConfiguration to be created.
     */
    public static class DummyComputation extends NoOpComputation<LongWritable,TwoVectorWritable, Writable,
        Writable> { }

    @Before
    public void setUp() {
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
        giraphConfiguration.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<LongWritable, TwoVectorWritable,
            Writable>(giraphConfiguration);
    }

    @Test
    public void testOuputFormat() throws IOException, InterruptedException {
        Text expected = new Text("[1,[4,5]]");

        JsonLongIDTwoVectorValueOutputFormatTestWorker(expected);
    }
  
    private void JsonLongIDTwoVectorValueOutputFormatTestWorker(Text expected) throws IOException,
        InterruptedException {
        TaskAttemptContext tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);

        Vertex vertex = mock(Vertex.class);
        when(vertex.getId()).thenReturn(new LongWritable(1L));
        when(vertex.getValue()).thenReturn(new TwoVectorWritable(new DenseVector(new double[]{2.0, 3.0}),
            new DenseVector(new double[]{4.0, 5.0})));

        // Create empty iterator == no edges
        when(vertex.getEdges()).thenReturn(new ArrayList<Text>());

        final RecordWriter<Text, Text> tw = mock(RecordWriter.class);
        JsonLongIDTwoVectorValueWriter writer = new JsonLongIDTwoVectorValueWriter() {
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
