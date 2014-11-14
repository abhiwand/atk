package com.intel.graphbuilder.io.titan.formats.hbase;

import com.intel.graphbuilder.io.titan.formats.util.TitanInputFormat;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.hbase.TitanHBaseHadoopGraph;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;
import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.TITAN_INPUT_VERSION;

/**
 * A temporary fix for Issue#817 KCVSLog$MessagePuller does not shut down when using the TitanInputFormat
 *
 * The Spark context does no shut down due to a runaway KCVSLog$MessagePuller thread that maintained a
 * connection to the underlying graph. Affects Titan 0.5.0 and 0.5.1. This code should be replaced once
 * Titan checks in a fix.
 *
 * @link https://github.com/thinkaurelius/titan/issues/817
 */
public class TitanHBaseRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private TableRecordReader reader;
    private TitanHBaseHadoopGraph graph;
    private FaunusVertexQueryFilter vertexQuery;
    private Configuration configuration;
    private TitanHadoopSetup titanSetup;
    private FaunusVertex vertex;

    private final byte[] edgestoreFamilyBytes;

    public TitanHBaseRecordReader(final ModifiableHadoopConfiguration faunusConf, final FaunusVertexQueryFilter vertexQuery, final TableRecordReader reader, final byte[] edgestoreFamilyBytes) {
        String titanVersion = faunusConf.get(TITAN_INPUT_VERSION);
        String className = TitanInputFormat.SETUP_PACKAGE_PREFIX + titanVersion + TitanInputFormat.SETUP_CLASS_NAME;

        this.titanSetup = ConfigurationUtil.instantiate(className, new Object[]{faunusConf.getHadoopConfiguration()}, new Class[]{Configuration.class});
        this.graph = new TitanHBaseHadoopGraph(titanSetup);
        this.vertexQuery = vertexQuery;
        this.reader = reader;
        this.edgestoreFamilyBytes = edgestoreFamilyBytes;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.reader.initialize(inputSplit, taskAttemptContext);
        this.configuration = DEFAULT_COMPAT.getContextConfiguration(taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (this.reader.nextKeyValue()) {
            final FaunusVertex temp = this.graph.readHadoopVertex(this.configuration, this.reader.getCurrentKey().copyBytes(), this.reader.getCurrentValue().getMap().get(edgestoreFamilyBytes));
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
