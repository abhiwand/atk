package com.intel.graphbuilder.io.titan.formats.cassandra;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;
import com.thinkaurelius.titan.hadoop.formats.cassandra.TitanCassandraHadoopGraph;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * A temporary fix for Issue#817 KCVSLog$MessagePuller does not shut down when using the TitanInputFormat
 *
 * The Spark context does no shut down due to a runaway KCVSLog$MessagePuller thread that maintained a
 * connection to the underlying graph. Affects Titan 0.5.0 and 0.5.1. This code should be replaced once
 * Titan checks in a fix.
 *
 * @link https://github.com/thinkaurelius/titan/issues/817
 */
public class TitanCassandraRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private static final Logger log =
            LoggerFactory.getLogger(com.thinkaurelius.titan.hadoop.formats.cassandra.TitanCassandraRecordReader.class);

    private ColumnFamilyRecordReader reader;
    private TitanCassandraHadoopGraph graph;
    private FaunusVertexQueryFilter vertexQuery;
    private Configuration configuration;
    private FaunusVertex vertex;

    public TitanCassandraRecordReader(final TitanCassandraHadoopGraph graph, final FaunusVertexQueryFilter vertexQuery, final ColumnFamilyRecordReader reader) {
        this.graph = graph;
        this.vertexQuery = vertexQuery;
        this.reader = reader;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.reader.initialize(inputSplit, taskAttemptContext);
        this.configuration = DEFAULT_COMPAT.getContextConfiguration(taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (this.reader.nextKeyValue()) {
            // TODO titan05 integration -- the duplicate() call may be unnecessary
            final FaunusVertex temp = this.graph.readHadoopVertex(this.configuration, this.reader.getCurrentKey().duplicate(), this.reader.getCurrentValue());
            if (null != temp) {
                this.vertex = temp;
                this.vertexQuery.filterRelationsOf(this.vertex);
                return true;
            }
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public FaunusVertex getCurrentValue() throws IOException, InterruptedException {
        return this.vertex;
    }

    @Override
    public void close() throws IOException {
        this.graph.close();
        this.reader.close();
    }

    @Override
    public float getProgress() {
        return this.reader.getProgress();
    }
}
