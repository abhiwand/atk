package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.intel.graphbuilder.titan.cache.TitanHadoopCacheConfiguration;
import com.intel.graphbuilder.titan.cache.TitanHadoopGraphCache;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * A patched version of the HBase record reader in Titan 0.5.2 which caches Titan graphs
 * because setting them up is very expensive.
 *
 * This code is a copy of TitanHBaseRecordReader with an added graph cache. Copying the class was
 * needed because there was no default constructor, and some variables that we need were private.
 */

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CachedTitanHBaseRecordReader extends RecordReader<NullWritable, FaunusVertex> {
    protected static TitanHadoopGraphCache graphCache = null;

    static {
        graphCache = new TitanHadoopGraphCache();
    }
    private TableRecordReader reader;
    private CachedTitanHBaseInputFormat inputFormat;
    private TitanHBaseHadoopGraph graph;
    private FaunusVertexQueryFilter vertexQuery;
    private Configuration configuration;
    private TaskAttemptContext taskAttemptContext;

    private FaunusVertex vertex;

    private final byte[] edgestoreFamilyBytes;

    public CachedTitanHBaseRecordReader(final CachedTitanHBaseInputFormat inputFormat, final FaunusVertexQueryFilter vertexQuery, final TableRecordReader reader, final byte[] edgestoreFamilyBytes) {
        TitanHadoopCacheConfiguration cacheConfiguration = new TitanHadoopCacheConfiguration(inputFormat);
        TitanHadoopSetup titanHadoopSetup = graphCache.getGraph(cacheConfiguration);

        this.inputFormat = inputFormat;
        this.vertexQuery = vertexQuery;
        this.reader = reader;
        this.edgestoreFamilyBytes = edgestoreFamilyBytes;
        graph = new TitanHBaseHadoopGraph(titanHadoopSetup);
    }


    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
        configuration = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(taskAttemptContext));
        this.taskAttemptContext = taskAttemptContext;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (reader.nextKeyValue()) {

            final FaunusVertex temp = graph.readHadoopVertex(
                    configuration,
                    reader.getCurrentKey().copyBytes(),
                    reader.getCurrentValue().getMap().get(edgestoreFamilyBytes));

            if (null != temp) {
                vertex = temp;
                vertexQuery.filterRelationsOf(vertex);
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
}
