//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.giraph.io.titan.formats;

import com.intel.giraph.io.titan.TitanGraphWriter;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.core.TitanGraph;
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
         * Vertex value properties to filter
         */
        protected String[] vertexValuePropertyKeyList = null;

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
        protected String regexp = "[\\s,\\t]+";

        /**
         * Initialize Titan vertex writer and open graph
         * @param context Task attempt context
         */
        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            this.graph = TitanGraphWriter.getGraphFromCache(context.getConfiguration());
            vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(context.getConfiguration()).split(regexp);
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
            //Closing the graph is managed by the graph cache
            //this.graph.shutdown();
            //LOG.info(CLOSED_GRAPH);
            super.close(context);
        }
    }
}
