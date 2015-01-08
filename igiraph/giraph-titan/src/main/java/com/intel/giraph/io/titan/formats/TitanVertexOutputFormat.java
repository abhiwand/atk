package com.intel.giraph.io.titan.formats;

import com.intel.giraph.io.DistanceMapWritable;
import com.intel.giraph.io.titan.TitanGraphWriter;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CLOSED_GRAPH;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.TITAN_MAX_VERTICES_PER_COMMIT;

public abstract class TitanVertexOutputFormat<I extends WritableComparable,
        V extends Writable, E extends Writable> extends TextVertexOutputFormat<I, V, E> {

    protected static final Logger LOG = Logger.getLogger(TitanVertexOutputFormat.class);


    /**
     * set up Titan based on users' configuration
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        GiraphTitanUtils.setupTitanOutput(conf);
        super.setConf(conf);
    }

    /**
     * Create Titan vertex writer
     *
     * @param context Job context
     * @return Titan vertex writer
     */

    public abstract TextVertexWriter createVertexWriter(TaskAttemptContext context);

    /**
     * Abstract class to be implemented by the user to write Giraph results to Titan via BluePrint API
     */
    protected abstract class TitanVertexWriterToEachLine extends TextVertexWriterToEachLine {

        /**
         * TitanFactory to write back results
         */
        protected TitanGraph graph = null;
        /**
         * Used to commit vertices in batches
         */
        protected int verticesPendingCommit = 0;
        /**
         * regular expression of the deliminators for a property list
         */
        protected String regexp = "[\\s,\\t]+";     //.split("/,?\s+/");

        /**
         * Initialize Titan vertex writer and open graph
         * @param context Task attempt context
         */
        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            this.graph = TitanGraphWriter.open(context);
        }

        /**
         * Write results to Titan vertex
         *
         * @param vertex Giraph vertex
         * @return   Text line to be written
         * @throws IOException
         */
        @Override
        public abstract Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException;

        public void commitVerticesInBatches() {
            this.verticesPendingCommit++;
            if (this.verticesPendingCommit % TITAN_MAX_VERTICES_PER_COMMIT == 0) {
                this.graph.commit();
                LOG.info("Committed " + this.verticesPendingCommit + " vertices to TianGraph");
            }
        }

        /**
         * Shutdown Titan graph
         *
         * @param context Task attempt context
         * @throws IOException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            this.graph.commit();
            this.graph.shutdown();
            LOG.info(CLOSED_GRAPH);
            super.close(context);
        }
    }
}