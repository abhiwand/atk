package com.intel.giraph.io.titan.hbase;

import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;

/**
 * Create RecordReader to read HBase row-key, result records.
 * Subclasses need to implement nextVertex() and getCurrentVertex()
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public abstract class TitanHBaseVertexReader<I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexReader<I, V, E> {

    protected RecordReader<NullWritable, FaunusVertex> recordReader;
    protected TaskAttemptContext context;


    /**
     * Create the hbase record recordReader. Override this to use a different
     * underlying record recordReader (useful for testing).
     *
     * @param inputSplit the split to read
     * @param context    the context passed to initialize
     * @return the record recordReader to be used
     * @throws java.io.IOException  exception that can be thrown during creation
     * @throws InterruptedException exception that can be thrown during
     *                              creation
     */
    protected RecordReader<NullWritable, FaunusVertex> createHBaseRecordReader(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        //BaseConfiguration titanConfiguration = GiraphToTitanGraphFactory.generateTitanConfiguration(context.getConfiguration(),
        //        GIRAPH_TITAN.get(context.getConfiguration()));
        TitanHBaseVertexInputFormat.INPUT_FORMAT.setConf(context.getConfiguration());
        return TitanHBaseVertexInputFormat.INPUT_FORMAT.createRecordReader(inputSplit, context);
    }

    /**
     * Initialize the Titan/HBase record reader
     *
     * @param inputSplit Input split to be used for reading vertices.
     * @param context Context from the task.
     * @throws IOException
     * @throws InterruptedException
     */

    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
            InterruptedException {
        this.recordReader = createHBaseRecordReader(inputSplit, context);
        this.context = context;

    }


    /**
     * Get progress of Titan/HBase record reader
     *
     * @return Progress of record reader
     * @throws IOException
     * @throws InterruptedException
     */
    public float getProgress() throws IOException, InterruptedException {
        return recordReader.getProgress();
    }

    /**
     * Get Titan/HBase record reader
     *
     * @return Record recordReader to be used for reading.
     */
    protected RecordReader<NullWritable, FaunusVertex> getRecordReader() {
        return recordReader;
    }

    /**
     * Get task context
     *
     * @return Context passed to initialize.
     */
    protected TaskAttemptContext getContext() {
        return context;
    }

    /**
     * Close the record reader
     *
     * @throws IOException
     */
    public void close() throws IOException {
        recordReader.close();
    }

}