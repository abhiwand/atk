package com.intel.graphbuilder.io.titan.formats.util;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.PIPELINE_TRACK_PATHS;
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
public abstract class TitanInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    public static final String SETUP_PACKAGE_PREFIX = "com.thinkaurelius.titan.hadoop.formats.util.input.";
    public static final String SETUP_CLASS_NAME = ".TitanHadoopSetupImpl";

    protected FaunusVertexQueryFilter vertexQuery;
    protected boolean trackPaths;
    //protected TitanHadoopSetup titanSetup;
    protected ModifiableHadoopConfiguration faunusConf;
    protected ModifiableConfiguration titanInputConf;

    @Override
    public void setConf(final Configuration config) {

        this.vertexQuery = FaunusVertexQueryFilter.create(config);

        this.faunusConf = ModifiableHadoopConfiguration.of(config);
        this.titanInputConf = faunusConf.getInputConf();
        //final String titanVersion = faunusConf.get(TITAN_INPUT_VERSION);
        this.trackPaths = faunusConf.get(PIPELINE_TRACK_PATHS);
        //final String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;

        //this.titanSetup = ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
    }
}
