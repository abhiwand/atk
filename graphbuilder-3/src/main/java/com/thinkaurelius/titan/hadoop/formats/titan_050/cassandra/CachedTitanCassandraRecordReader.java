package com.thinkaurelius.titan.hadoop.formats.titan_050.cassandra;

import com.intel.graphbuilder.titan.cache.TitanHadoopCacheConfiguration;
import com.intel.graphbuilder.titan.cache.TitanHadoopGraphCache;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.cassandra.TitanCassandraHadoopGraph;
import com.thinkaurelius.titan.hadoop.formats.titan_050.util.CachedTitanInputFormat;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
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
import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.TITAN_INPUT_VERSION;

/**
 * A temporary fix for Issue#817 KCVSLog$MessagePuller does not shut down when using the TitanInputFormat
 *
 * The Spark context does no shut down due to a runaway KCVSLog$MessagePuller thread that maintained a
 * connection to the underlying graph. Affects Titan 0.5.0 and 0.5.1. The bug was fixed in Titan 0.5.2.
 * However, we could not upgrade due to an issue in Titan 0.5.2 that caused bulk reads to hang on large datasets.
 *
 * This code is a copy of TitanCassandraRecordReader in Titan 0.5.0 with an added graph cache.
 *
 * @link https://github.com/thinkaurelius/titan/issues/817
 */
public class CachedTitanCassandraRecordReader extends RecordReader<NullWritable, FaunusVertex> {
    private static TitanHadoopGraphCache graphCache = null;

    static {
        graphCache = new TitanHadoopGraphCache();

        Runtime.getRuntime().addShutdownHook(new Thread() { //Needed to shutdown any runaway threads
            @Override
            public void run() {
                log.info("Invalidating Titan/HBase graph cache");
                invalidateGraphCache();
            }
        });
    }
    private static final Logger log =
            LoggerFactory.getLogger(CachedTitanCassandraRecordReader.class);

    private ColumnFamilyRecordReader reader;
    private TitanCassandraHadoopGraph graph;
    private FaunusVertexQueryFilter vertexQuery;
    private Configuration configuration;
    private FaunusVertex vertex;

    public CachedTitanCassandraRecordReader(final ModifiableHadoopConfiguration faunusConf, final FaunusVertexQueryFilter vertexQuery, final ColumnFamilyRecordReader reader) {
        TitanHadoopCacheConfiguration cacheConfiguration = new TitanHadoopCacheConfiguration(faunusConf);
        TitanHadoopSetup titanSetup = graphCache.getGraph(cacheConfiguration);

        //String titanVersion = faunusConf.get(TITAN_INPUT_VERSION);
        //String className = TitanInputFormat.SETUP_PACKAGE_PREFIX + titanVersion + TitanInputFormat.SETUP_CLASS_NAME;
        //this.titanSetup = ConfigurationUtil.instantiate(className, new Object[]{faunusConf.getHadoopConfiguration()}, new Class[]{Configuration.class});

        this.graph = new TitanCassandraHadoopGraph(titanSetup);
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
        //Closing the graph is managed by the graph cache
        //this.graph.close();
        this.reader.close();
    }

    @Override
    public float getProgress() {
        return this.reader.getProgress();
    }

    /**
     * Invalidate all entries in the graph cache when the JVM shuts down.
     * This prevents any run-away message puller threads from maintaining a connection to the key-value store.
     */
    public static void invalidateGraphCache() {
        if (graphCache != null) {
            graphCache.invalidateAllCacheEntries();
        }
    }
}