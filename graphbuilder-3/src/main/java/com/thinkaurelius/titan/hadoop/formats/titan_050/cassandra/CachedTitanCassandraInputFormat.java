package com.thinkaurelius.titan.hadoop.formats.titan_050.cassandra;

import com.thinkaurelius.titan.hadoop.formats.titan_050.util.CachedTitanInputFormat;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * A temporary fix for Issue#817 KCVSLog$MessagePuller does not shut down when using the TitanInputFormat
 *
 * The Spark context does no shut down due to a runaway KCVSLog$MessagePuller thread that maintained a
 * connection to the underlying graph. Affects Titan 0.5.0 and 0.5.1. The bug was fixed in
 * Titan 0.5.2. However, we could not upgrade due to an issue in Titan 0.5.2 that caused bulk reads to hang on large datasets.
 *
 * This code is a copy of CachedTitanCassandraInputFormat in Titan 0.5.0 that uses CachedTitanCassandraRecordReader.
 *
 * @link https://github.com/thinkaurelius/titan/issues/817
 */
public class CachedTitanCassandraInputFormat extends CachedTitanInputFormat {
    private static final StaticBuffer DEFAULT_COLUMN = StaticArrayBuffer.of(new byte[0]);
    private static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);

    // Copied these private constants from Cassandra's ConfigHelper circa 2.0.9
    private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
    private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";

    private final ColumnFamilyInputFormat columnFamilyInputFormat = new ColumnFamilyInputFormat();
    private Configuration config;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.columnFamilyInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        return new CachedTitanCassandraRecordReader(this.faunusConf, this.vertexQuery,
                (ColumnFamilyRecordReader) this.columnFamilyInputFormat.createRecordReader(inputSplit, taskAttemptContext));
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);

        // Copy some Titan configuration keys to the Hadoop Configuration keys used by Cassandra's ColumnFamilyInputFormat
        ConfigHelper.setInputInitialAddress(config, titanInputConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
        if (titanInputConf.has(GraphDatabaseConfiguration.STORAGE_PORT))
            ConfigHelper.setInputRpcPort(config, String.valueOf(titanInputConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        if (titanInputConf.has(GraphDatabaseConfiguration.AUTH_USERNAME))
            ConfigHelper.setInputKeyspaceUserName(config, titanInputConf.get(GraphDatabaseConfiguration.AUTH_USERNAME));
        if (titanInputConf.has(GraphDatabaseConfiguration.AUTH_PASSWORD))
            ConfigHelper.setInputKeyspacePassword(config, titanInputConf.get(GraphDatabaseConfiguration.AUTH_PASSWORD));

        // Copy keyspace, force the CF setting to edgestore, honor widerows when set
        final boolean wideRows = config.getBoolean(INPUT_WIDEROWS_CONFIG, false);
        // Use the setInputColumnFamily overload that includes a widerows argument; using the overload without this argument forces it false
        ConfigHelper.setInputColumnFamily(config, titanInputConf.get(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE), Backend.EDGESTORE_NAME, wideRows);

        // Set the column slice bounds via Faunus's vertex query filter
        final SlicePredicate predicate = new SlicePredicate();
        final int rangeBatchSize = config.getInt(RANGE_BATCH_SIZE_CONFIG, Integer.MAX_VALUE);
        predicate.setSlice_range(getSliceRange(DEFAULT_SLICE_QUERY, rangeBatchSize));
        ConfigHelper.setInputSlicePredicate(config, predicate);
        ConfigHelper.setInputPartitioner(config, "Murmur3Partitioner");
        this.config = config;
    }

    private SliceRange getSliceRange(final SliceQuery slice, final int limit) {
        final SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(slice.getSliceStart().asByteBuffer());
        sliceRange.setFinish(slice.getSliceEnd().asByteBuffer());
        sliceRange.setCount(Math.min(limit, slice.getLimit()));
        return sliceRange;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }
}
