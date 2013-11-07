//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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
package com.intel.giraph.io.titan;

import com.intel.giraph.io.DistanceMapWritable;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.*;

/**
 * The Vertex Output Format which writes back Giraph algorithm results
 * to Titan.
 * <p/>
 * Each Vertex is with <code>Long</code> id,
 * and <code>DistanceMap</code> values.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class TitanVertexOutputFormatLongIDDistanceMap <I extends LongWritable,
            V extends DistanceMapWritable, E extends NullWritable>
    extends TextVertexOutputFormat<I, V, E> {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanVertexOutputFormatLongIDDistanceMap.class);


    /**
     * set up Titan with based on users' configuration
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        sanityCheckInputParameters(conf);
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        super.setConf(conf);
    }

    /**
     * check whether input parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public void sanityCheckInputParameters(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        String[] vertexPropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(",");
        if (vertexPropertyKeyList.length == 0) {
            throw new IllegalArgumentException("Please configure output vertex property list by -D" +
                    OUTPUT_VERTEX_PROPERTY_KEY_LIST.getKey() + ". Otherwise no vertex result will be written.");
        }

        if (GIRAPH_TITAN_STORAGE_BACKEND.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan storage backend by -D" +
                    GIRAPH_TITAN_STORAGE_BACKEND.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (GIRAPH_TITAN_STORAGE_TABLENAME.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan storage Table name by -D" +
                    GIRAPH_TITAN_STORAGE_TABLENAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan storage hostname by -D" +
                    GIRAPH_TITAN_STORAGE_HOSTNAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (GIRAPH_TITAN_STORAGE_PORT.isDefaultValue(conf)) {
            LOG.info(GIRAPH_TITAN_STORAGE_PORT.getKey() + " is configured as default value. " +
                    "Ensure you are using port " + GIRAPH_TITAN_STORAGE_PORT.get(conf));
        }

        if (GIRAPH_TITAN_STORAGE_READ_ONLY.get(conf).equals("true")) {
            throw new IllegalArgumentException("Please turnoff Titan storage read-only by -D" +
                    GIRAPH_TITAN_STORAGE_READ_ONLY.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (VERTEX_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info("No vertex type property specified. Ensure your " +
                    "InputFormat does not require one.");
        }

        if (EDGE_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info("No edge type property specified. Ensure your " +
                    "InputFormat does not require one.");
        }
    }

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new TitanHBaseLongIDDistanceMapWriter();
    }

    /**
     * VertexWriter that writes Giraph results to Titan via BluePrint API
     * vertices with <code>Long</code> id
     * and <code>TwoVector</code> values.
     */
    protected class TitanHBaseLongIDDistanceMapWriter extends TextVertexWriterToEachLine {

        /**
         * reader to parse Titan graph
         */
        private TitanGraph graph;
        /**
         * Vertex properties to filter
         */
        private String[] vertexPropertyKeyList;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            this.graph = TitanGraphWriter.open(context);
            assert (null != this.graph);
            vertexPropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(context.getConfiguration()).split(",");
            for (int i = 0; i < vertexPropertyKeyList.length; i++) {
                LOG.info("create vertex.property in Titan " + vertexPropertyKeyList[i]);
                 this.graph.makeKey(vertexPropertyKeyList[i]).dataType(String.class).make();
            }
        }

        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {

            long vertex_id = vertex.getId().get();
            String destinationVidStr = vertex.getId().toString();
            long numSources = 0;
            long sumHopCounts = 0;
            com.tinkerpop.blueprints.Vertex bluePrintVertex = this.graph.getVertex(vertex_id);
            HashMap<Long, Integer> distanceMap = vertex.getValue().getDistanceMap();

            for (Map.Entry<Long, Integer> entry : distanceMap.entrySet()) {
                numSources++;
                sumHopCounts += entry.getValue();
            }

            bluePrintVertex.setProperty(vertexPropertyKeyList[0], Long.toString(numSources));
            bluePrintVertex.setProperty(vertexPropertyKeyList[1], Long.toString(sumHopCounts));
            return null;
        }

        @Override
        public void close(TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.graph.commit();
        }
    }
}
