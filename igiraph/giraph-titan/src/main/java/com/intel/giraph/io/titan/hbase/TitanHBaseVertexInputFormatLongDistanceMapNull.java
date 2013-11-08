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
package com.intel.giraph.io.titan.hbase;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import com.intel.giraph.io.DistanceMapWritable;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.util.Base64;

import com.thinkaurelius.titan.diskstorage.Backend;

import java.io.IOException;
import java.nio.charset.Charset;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.*;

/**
 * TitanHBaseVertexInputFormatLongDistanceMapNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>DistanceMap</code> vertex values,
 * and <code>Null</code> edge weights.
 */
public class TitanHBaseVertexInputFormatLongDistanceMapNull extends
        TitanHBaseVertexInputFormat<LongWritable, DistanceMapWritable, NullWritable> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatLongDistanceMapNull.class);

    /**
     * checkInputSpecs
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void checkInputSpecs(Configuration conf) {
    }

    /**
     * set up HBase with based on users' configuration
     *
     * @param conf :Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, DistanceMapWritable, NullWritable> conf) {
        sanityCheckInputParameters(conf);
        conf.set(TableInputFormat.INPUT_TABLE, GIRAPH_TITAN_STORAGE_TABLENAME.get(conf));
        conf.set(HConstants.ZOOKEEPER_QUORUM, GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf));
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, GIRAPH_TITAN_STORAGE_PORT.get(conf));

        Scan scan = new Scan();
        scan.addFamily(Backend.EDGESTORE_NAME.getBytes(Charset.forName("UTF-8")));

        try {
            conf.set(TableInputFormat.SCAN, convertScanToString(scan));
        } catch (IOException e) {
            LOG.error("cannot write scan into a Base64 encoded string!");
        }

        super.setConf(conf);
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    static String convertScanToString(Scan scan) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        scan.write(dos);
        return Base64.encodeBytes(out.toByteArray());
    }

    /**
     * check whether input parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public void sanityCheckInputParameters(ImmutableClassesGiraphConfiguration<LongWritable, DistanceMapWritable, NullWritable> conf) {
        String tableName = GIRAPH_TITAN_STORAGE_TABLENAME.get(conf);

        if (GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan/HBase storage hostname by -D" +
                    GIRAPH_TITAN_STORAGE_HOSTNAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (tableName.equals("")) {
            throw new IllegalArgumentException("Please configure Titan/HBase Table name by -D" +
                    GIRAPH_TITAN_STORAGE_TABLENAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        } else {
            try {
                HBaseConfiguration config = new HBaseConfiguration();
                HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
                if (!hbaseAdmin.tableExists(tableName)) {
                    throw new IllegalArgumentException("HBase table " + tableName +
                            " does not exist! Please double check your configuration.");
                }

                if (!hbaseAdmin.isTableAvailable(tableName)) {
                    throw new IllegalArgumentException("HBase table " + tableName +
                            " is not available! Please double check your configuration.");
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to connect to HBase table " + tableName);
            }
        }


        if (GIRAPH_TITAN_STORAGE_PORT.isDefaultValue(conf)) {
            LOG.info(GIRAPH_TITAN_STORAGE_PORT.getKey() + " is configured as default value. " +
                    "Ensure you are using port " + GIRAPH_TITAN_STORAGE_PORT.get(conf));

        }

        if (INPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).equals("")) {
            LOG.info("No input vertex property list specified. Ensure your " +
                    "InputFormat does not require one.");
        }

        if (INPUT_EDGE_PROPERTY_KEY_LIST.get(conf).equals("")) {
            LOG.info("No input edge property list specified. Ensure your " +
                    "InputFormat does not require one.");
        }

        if (INPUT_EDGE_LABEL_LIST.get(conf).equals("")) {
            LOG.info("No input edge label specified. Ensure your " +
                    "InputFormat does not require one.");
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

    /**
     * create TitanHBaseVertexReader
     *
     * @param split   : inputsplits from TableInputFormat
     * @param context : task context
     * @return VertexReader
     * @throws IOException
     * @throws RuntimeException
     */
    public VertexReader<LongWritable, DistanceMapWritable, NullWritable> createVertexReader(InputSplit split,
                                                                                            TaskAttemptContext context) throws IOException {

        return new TitanHBaseVertexReader(split, context);

    }

    /**
     * Uses the RecordReader to return HBase data
     */
    public static class TitanHBaseVertexReader extends
            HBaseVertexReader<LongWritable, DistanceMapWritable, NullWritable> {
        /**
         * Graph Reader to parse data in Titan Graph semantics
         */
        private TitanHBaseGraphReader graphReader;
        /**
         * Giraph Veretex
         */
        private Vertex vertex;
        /**
         * task context
         */
        private final TaskAttemptContext context;

        /**
         * TitanHBaseVertexReader constructor
         *
         * @param split   InputSplit from TableInputFormat
         * @param context task context
         * @throws IOException
         */
        public TitanHBaseVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            this.context = context;
        }

        /**
         * @param inputSplit Input Split form HBase
         * @param context    task context
         */
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(inputSplit, context);
            this.graphReader = new TitanHBaseGraphReader(
                    GiraphToTitanGraphFactory.generateTitanConfiguration(context.getConfiguration(),
                            GIRAPH_TITAN.get(context.getConfiguration())));
        }

        /**
         * check whether these is nextVertex available
         *
         * @return boolean
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            if (getRecordReader().nextKeyValue()) {
                final Vertex temp = graphReader.readGiraphVertex(LONG_DISTANCE_MAP_NULL, getConf(),
                        getRecordReader().getCurrentKey().copyBytes(),
                        getRecordReader().getCurrentValue().getMap().get(EDGE_STORE_FAMILY));
                if (null != temp) {
                    vertex = temp;
                    return true;
                } else if (getRecordReader().nextKeyValue()) {
                    final Vertex temp1 = graphReader.readGiraphVertex(LONG_DISTANCE_MAP_NULL, getConf(),
                            getRecordReader().getCurrentKey().copyBytes(),
                            getRecordReader().getCurrentValue().getMap().get(EDGE_STORE_FAMILY));
                    if (null != temp1) {
                        vertex = temp1;
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * getCurrentVetex
         *
         * @return Vertex : Giraph vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Vertex<LongWritable, DistanceMapWritable, NullWritable> getCurrentVertex() throws IOException,
                InterruptedException {
            return vertex;
        }
    }
}
