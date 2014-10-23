//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.Backend;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;


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
        String regexp = "[\\s,\\t]+";
        String[] vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(regexp);
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
     * @param conf : Giraph configuration
     * @TODO: Change this to match new schema
     * disable Hadoop speculative execution.
     */
    public static void createTitanKeys(ImmutableClassesGiraphConfiguration conf) {
        TitanGraph graph;
        String regexp = "[\\s,\\t]+";

        try {
            graph = TitanGraphWriter.open(conf);
        } catch (IOException e) {
            LOG.error(TITAN_GRAPH_NOT_OPEN);
            throw new RuntimeException(TITAN_GRAPH_NOT_OPEN);
        }

        String[] vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(regexp);

        // TODO: Need locks?
        TitanManagement graphManager =  graph.getManagementSystem();

        for (int i = 0; i < vertexValuePropertyKeyList.length; i++) {
            if (!graphManager.containsRelationType(vertexValuePropertyKeyList[i])) {
                LOG.info(CREATE_VERTEX_PROPERTY + vertexValuePropertyKeyList[i]);
                //for titan 0.5.0+
                graphManager.makePropertyKey(vertexValuePropertyKeyList[i]).dataType(String.class).make();
            }
        }
        graphManager.commit();
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
