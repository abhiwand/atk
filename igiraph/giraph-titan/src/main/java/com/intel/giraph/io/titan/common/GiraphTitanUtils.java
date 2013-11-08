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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import com.thinkaurelius.titan.diskstorage.Backend;

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
     * check whether output parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public static void sanityCheckOutputParameters(ImmutableClassesGiraphConfiguration conf) {
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
     * @param scan The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    public static void configHBase(ImmutableClassesGiraphConfiguration conf) throws IOException {
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
    }

    /**
     * set up configuration for Titan/HBase.
     *
     * @param conf : Giraph configuratio
     * @throws IOException When writing the scan fails.
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
        disableSpeculativeExe(conf);
    }
}
