package com.intel.giraph.io.titan.formats;

import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.formats.titan_050.util.CachedTitanInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Create RecordReader to read Faunus vertices from HBase
 * Subclasses need to implement nextVertex() and getCurrentVertex()
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public abstract class TitanVertexReader<I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexReader<I, V, E> {

    protected TitanVertexBuilder vertexBuilder = null;
    protected CachedTitanInputFormat titanInputFormat = null;
    private final RecordReader<NullWritable, FaunusVertex> recordReader;
    private TaskAttemptContext context;
    private final Logger LOG = Logger.getLogger(TitanVertexReader.class);

    /**
     * Sets the Titan/HBase TableInputFormat and creates a record reader.
     *
     * @param split   InputSplit
     * @param context Context
     * @throws java.io.IOException
     */
    public TitanVertexReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        Configuration conf = context.getConfiguration();
        GiraphToTitanGraphFactory.addFaunusInputConfiguration(conf);

        this.vertexBuilder = new TitanVertexBuilder(conf);
        this.titanInputFormat = TitanInputFormatFactory.getTitanInputFormat(conf);
        this.titanInputFormat.setConf(conf);

        try {
            this.recordReader = this.titanInputFormat.createRecordReader(split, context);
        } catch (InterruptedException e) {
            LOG.error("Interruption while creating record reader for Titan/HBase", e);
            throw new IOException(e);
        }
    }

    /**
     * Initialize the record reader
     *
     * @param inputSplit Input split to be used for reading vertices.
     * @param context    Context from the task.
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.recordReader.initialize(inputSplit, context);
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
        return this.recordReader.getProgress();
    }

    /**
     * Get Titan/HBase record reader
     *
     * @return Record recordReader to be used for reading.
     */
    protected RecordReader<NullWritable, FaunusVertex> getRecordReader() {
        return this.recordReader;
    }

    /**
     * Get task context
     *
     * @return Context passed to initialize.
     */
    protected TaskAttemptContext getContext() {
        return this.context;
    }

    /**
     * Close the Titan/HBase record reader
     *
     * @throws IOException
     */
    public void close() throws IOException {
        this.recordReader.close();
    }

}
