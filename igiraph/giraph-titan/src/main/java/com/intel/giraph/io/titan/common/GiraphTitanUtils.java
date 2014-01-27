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
package com.intel.giraph.io.titan.common;

import com.intel.giraph.io.titan.TitanGraphWriter;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.util.FakeLock;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.locks.Lock;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CONFIGURED_DEFAULT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CONFIG_PREFIX;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CONFIG_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CONFIG_VERTEX_PROPERTY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CREATE_VERTEX_PROPERTY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.DOUBLE_CHECK_CONFIG;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.ENSURE_INPUT_FORMAT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.ENSURE_PORT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.FAILED_CONNECT_HBASE_TABLE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_READ_ONLY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_VALUE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.NO_EDGE_LABEL;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.NO_EDGE_PROPERTY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.NO_EDGE_TYPE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.NO_VERTEX_PROPERTY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.NO_VERTEX_READ;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.NO_VERTEX_TYPE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OPENED_GRAPH;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.TITAN_GRAPH_NOT_OPEN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.TITAN_TX_NOT_OPEN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_PROPERTY_KEY;


/**
 * Utility methods for Titan IO
 */
public class GiraphTitanUtils {
    /**
     * Logger
     */
    private static final Logger LOG = Logger.getLogger(GiraphTitanUtils.class);

    /**
     * Do not instantiate
     */
    private GiraphTitanUtils() {
    }

    /**
     * check whether input parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public static void sanityCheckInputParameters(ImmutableClassesGiraphConfiguration conf) {
        String tableName = GIRAPH_TITAN_STORAGE_TABLENAME.get(conf);

        if (GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf).equals("")) {
            throw new IllegalArgumentException(CONFIG_TITAN + "host name" + CONFIG_PREFIX +
                GIRAPH_TITAN_STORAGE_HOSTNAME.getKey() + NO_VERTEX_READ);
        }

        if (tableName.equals("")) {
            throw new IllegalArgumentException(CONFIG_TITAN + "table name" + CONFIG_PREFIX +
                GIRAPH_TITAN_STORAGE_TABLENAME.getKey() + NO_VERTEX_READ);
        } else {
            try {
                HBaseConfiguration config = new HBaseConfiguration();
                HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
                if (!hbaseAdmin.tableExists(tableName)) {
                    throw new IllegalArgumentException("HBase table " + tableName +
                        " does not exist! " + DOUBLE_CHECK_CONFIG);
                }

                if (!hbaseAdmin.isTableAvailable(tableName)) {
                    throw new IllegalArgumentException("HBase table " + tableName +
                        " is not available! " + DOUBLE_CHECK_CONFIG);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException(FAILED_CONNECT_HBASE_TABLE + tableName);
            }
        }


        if (GIRAPH_TITAN_STORAGE_PORT.isDefaultValue(conf)) {
            LOG.info(GIRAPH_TITAN_STORAGE_PORT.getKey() + CONFIGURED_DEFAULT +
                ENSURE_PORT + GIRAPH_TITAN_STORAGE_PORT.get(conf));

        }

        if (INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.get(conf).equals("")) {
            LOG.info(NO_VERTEX_PROPERTY + ENSURE_INPUT_FORMAT);
        }

        if (INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.get(conf).equals("")) {
            LOG.info(NO_EDGE_PROPERTY + ENSURE_INPUT_FORMAT);
        }

        if (INPUT_EDGE_LABEL_LIST.get(conf).equals("")) {
            LOG.info(NO_EDGE_LABEL + ENSURE_INPUT_FORMAT);
        }

        if (VERTEX_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info(NO_VERTEX_TYPE + ENSURE_INPUT_FORMAT);
        }

        if (EDGE_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info(NO_EDGE_TYPE + ENSURE_INPUT_FORMAT);
        }
    }

    /**
     * check whether output parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public static void sanityCheckOutputParameters(ImmutableClassesGiraphConfiguration conf) {
        String[] vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(",");
        if (vertexValuePropertyKeyList.length == 0) {
            throw new IllegalArgumentException(CONFIG_VERTEX_PROPERTY + CONFIG_PREFIX +
                OUTPUT_VERTEX_PROPERTY_KEY_LIST.getKey() + NO_VERTEX_READ);
        }

        if (GIRAPH_TITAN_STORAGE_BACKEND.get(conf).equals("")) {
            throw new IllegalArgumentException(CONFIG_TITAN + "backend" + CONFIG_PREFIX +
                GIRAPH_TITAN_STORAGE_BACKEND.getKey() + NO_VERTEX_READ);
        }

        if (GIRAPH_TITAN_STORAGE_TABLENAME.get(conf).equals("")) {
            throw new IllegalArgumentException(CONFIG_TITAN + "table name" + CONFIG_PREFIX +
                GIRAPH_TITAN_STORAGE_TABLENAME.getKey() + NO_VERTEX_READ);
        }

        if (GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf).equals("")) {
            throw new IllegalArgumentException(CONFIG_TITAN + "host name" + CONFIG_PREFIX +
                GIRAPH_TITAN_STORAGE_HOSTNAME.getKey() + NO_VERTEX_READ);
        }

        if (GIRAPH_TITAN_STORAGE_PORT.isDefaultValue(conf)) {
            LOG.info(GIRAPH_TITAN_STORAGE_PORT.getKey() + CONFIGURED_DEFAULT +
                ENSURE_PORT + GIRAPH_TITAN_STORAGE_PORT.get(conf));
        }

        if (GIRAPH_TITAN_STORAGE_READ_ONLY.get(conf).equals("true")) {
            throw new IllegalArgumentException(CONFIG_TITAN + "read only" + CONFIG_PREFIX +
                GIRAPH_TITAN_STORAGE_READ_ONLY.getKey() + NO_VERTEX_READ);
        }

        if (VERTEX_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info(NO_VERTEX_TYPE + ENSURE_INPUT_FORMAT);
        }

        if (EDGE_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info(NO_EDGE_TYPE + ENSURE_INPUT_FORMAT);
        }
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    public static String convertScanToString(Scan scan) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        scan.write(dos);
        return Base64.encodeBytes(out.toByteArray());
    }


    /**
     * Configure HBase for Titan input.
     *
     * @param conf Giraph configuration
     * @throws IOException When writing the scan fails.
     */
    public static void configHBase(ImmutableClassesGiraphConfiguration conf) throws IOException {
        conf.set(TableInputFormat.INPUT_TABLE, GIRAPH_TITAN_STORAGE_TABLENAME.get(conf));
        conf.set(HConstants.ZOOKEEPER_QUORUM, GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf));
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, GIRAPH_TITAN_STORAGE_PORT.get(conf));

        Scan scan = new Scan();
        scan.addFamily(Backend.EDGESTORE_NAME.getBytes(Charset.forName("UTF-8")));
        conf.set(TableInputFormat.SCAN, convertScanToString(scan));
    }

    /**
     * set up configuration for Titan/HBase.
     *
     * @param conf : Giraph configuration
     */
    public static void setupHBase(ImmutableClassesGiraphConfiguration conf) {
        try {
            sanityCheckInputParameters(conf);
            configHBase(conf);
        } catch (IOException e) {
            LOG.error("IO exception when configure HBase!");
        }
    }

    /**
     * disable Hadoop speculative execution.
     *
     * @param conf : Giraph configuration
     */
    public static void createTitanKeys(ImmutableClassesGiraphConfiguration conf) {
        TitanGraph graph;

        try {
            graph = TitanGraphWriter.open(conf);
        } catch (IOException e) {
            LOG.error(TITAN_GRAPH_NOT_OPEN);
            throw new RuntimeException(TITAN_GRAPH_NOT_OPEN);
        }

        TitanTransaction tx = graph.newTransaction();
        if (tx == null) {
            LOG.error(TITAN_TX_NOT_OPEN);
            throw new RuntimeException(TITAN_TX_NOT_OPEN);
        }
        LOG.info(OPENED_GRAPH);
        String[] vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(",");
        Lock dbLock = FakeLock.INSTANCE;
        dbLock.lock();
        for (int i = 0; i < vertexValuePropertyKeyList.length; i++) {
            if (!tx.containsType(vertexValuePropertyKeyList[i])) {
                LOG.info(CREATE_VERTEX_PROPERTY + vertexValuePropertyKeyList[i]);
                // for titan 0.3.2
                //     this.graph.makeType().name().unique(Direction.OUT).dataType(String.class)
                //             .makePropertyKey();
                //for titan 0.4.0
                graph.makeKey(vertexValuePropertyKeyList[i]).dataType(String.class).make();
            }
        }
        dbLock.unlock();
        if (tx.isOpen()) {
            tx.commit();
        }
        graph.commit();
        graph.shutdown();
    }


    /**
     * disable Hadoop speculative execution.
     *
     * @param conf : Giraph configuration
     */
    public static void disableSpeculativeExe(ImmutableClassesGiraphConfiguration conf) {
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
    }

    /**
     * set up configuration for Titan Output
     *
     * @param conf : Giraph configuratio
     */
    public static void setupTitanOutput(ImmutableClassesGiraphConfiguration conf) {
        sanityCheckOutputParameters(conf);
        createTitanKeys(conf);
        disableSpeculativeExe(conf);
    }
}
