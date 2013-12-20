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

import org.apache.giraph.conf.StrConfOption;


/**
 * Constants used all over Giraph for configuration specific for Titan/Hbase
 * Titan/Cassandra
 */

public class GiraphTitanConstants {

    /**
     * Titan backend type .
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_BACKEND = new StrConfOption(
        "giraph.titan.input.storage.backend", "", "Titan backend - required");
    /**
     * Titan Storage hostname .
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_HOSTNAME = new StrConfOption(
        "giraph.titan.input.storage.hostname", "", "Titan/Hbase hostname - required");
    /**
     * Titan Stroage table name .
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_TABLENAME = new StrConfOption(
        "giraph.titan.input.storage.tablename", "", "Titan/Hbase tablename - required");
    /**
     * port where to contact Titan storage.
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_PORT = new StrConfOption(
        "giraph.titan.input.storage.port", "2181", "port where to contact Titan/hbase");
    /**
     * Titan storage connection time out.
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_CONNECTION_TIMEOUT = new StrConfOption(
        "giraph.titan.input.storage.connection-timeout", "100000", "Titan/hbase storage time out");
    /**
     * Titan storage batch loading.
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_BATCH_LOADING = new StrConfOption(
        "giraph.titan.input.storage.batch-loading", "true", "Titan storage batch-loading");
    /**
     * Titan storage ids block-size.
     */
    public static final StrConfOption GIRAPH_TITAN_IDS_BLOCK_SIZE = new StrConfOption(
        "giraph.titan.input.storage.ids.block-size", "50000", "Titan ids block size");
    /*/**
     * Titan Storage attempt-wait.
     */
    /*public static final StrConfOption GIRAPH_TITAN_STORAGE_ATTEMPT_WAIT = new StrConfOption(
            "giraph.titan.input.storage.attempt-wait", "750", "Titan storage attempt wait");
    /**
     * Titan Storage idauthority-wait-time.
     */
    /* static final StrConfOption GIRAPH_TITAN_STORAGE_IDAUTORITY_WAIT_TIME = new StrConfOption(
            "giraph.titan.input.storage.idauthority-wait-time", "3000", "Titan storage idauthority wait time");
    /**
     * Titan Storage write attempts
     */
    /*public static final StrConfOption GIRAPH_TITAN_STORAGE_WRITE_ATTEMPTS = new StrConfOption(
            "giraph.titan.input.storage.write-attempts", "10", "Titan storage write attempts");
    /**
     * Titan Storage lock wait time.
     */
    /*public static final StrConfOption GIRAPH_TITAN_STORAGE_LOCK_WAIT_TIME = new StrConfOption(
            "giraph.titan.input.storage.lock-wait-time", "500", "Titan storage lock wait time");
    /**
     * Titan Storage lock expiry time
     */
    /*public static final StrConfOption GIRAPH_TITAN_STORAGE_LOCK_EXPIRY_TIME = new StrConfOption(
            "giraph.titan.input.storage.locak-expiry-time", "600000", "Titan storage lock expiry time");
    /**
     * Titan Storage ids renew-timeout.
     */
    /*public static final StrConfOption GIRAPH_TITAN_IDS_RENEW_TIMEOUT = new StrConfOption(
            "giraph.titan.input.ids.renew-timeout", "600000", "Titan ids renew timeout");
    /**
     * Titan Storage ids partition.
     */
    /*public static final StrConfOption GIRAPH_TITAN_IDS_PARTITION = new StrConfOption(
            "giraph.titan.input.ids.partition", "false", "Titan ids partition");
    /**
     * Titan Storage ids num-partition
     */
    /*public static final StrConfOption GIRAPH_TITAN_IDS_NUM_PARTITIONS = new StrConfOption(
            "giraph.titan.input.ids.num-partitions", "100", "Titan ids num partitions");     */
    /**
     * the configuration prefix to stripped for Titan
     */
    public static final StrConfOption GIRAPH_TITAN = new StrConfOption("giraph.titan", "giraph.titan.input",
        "Giraph/Titan prefix");
    /**
     * Storage backend
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_READ_ONLY = new StrConfOption(
        "giraph.titan.input.storage.read-only", "false", "read only or not");
    /**
     * backend autotype
     */
    public static final StrConfOption GIRAPH_TITAN_AUTOTYPE = new StrConfOption(
        "giraph.titan.input.autotype", "none", "autotype");
    /**
     * the list of vertex properties to filter during data loading from Titan
     */
    public static final StrConfOption INPUT_VERTEX_PROPERTY_KEY_LIST = new StrConfOption(
        "input.vertex.property.key.list", "", "the vertex property keys which Giraph reads from Titan");
    /**
     * the list of edge properties to filter during data loading from Titan
     */
    public static final StrConfOption INPUT_EDGE_PROPERTY_KEY_LIST = new StrConfOption("input.edge.property.key.list",
        "", "the edge property keys which Giraph needs");
    /**
     * the list of edge labels to filter during data loading from titan
     */
    public static final StrConfOption INPUT_EDGE_LABEL_LIST = new StrConfOption("input.edge.label.list", "",
        "the edge labels which Giraph needs");
    /**
     * the list of vertex properties to write results back to Titan
     */
    public static final StrConfOption OUTPUT_VERTEX_PROPERTY_KEY_LIST = new StrConfOption(
        "output.vertex.property.key.list", "", "the vertex property keys which Giraph writes back to Titan");
    /**
     * the property key for vertex type
     */
    public static final StrConfOption VERTEX_TYPE_PROPERTY_KEY = new StrConfOption(
        "vertex.type.property.key", "", "the property key for vertex type");
    /**
     * the property key for edge type
     */
    public static final StrConfOption EDGE_TYPE_PROPERTY_KEY = new StrConfOption(
        "edge.type.property.key", "", "the property key for edge type");
    /**
     * whether to output bias for each vertex
     * when bias is enabled, the last property name in the OUTPUT_VERTEX_PROPERTY_KEY_LIST
     * is for bias
     */
    public static final StrConfOption OUTPUT_VERTEX_BIAS = new StrConfOption(
        "output.vertex.bias", "false", "whether to output vertex bias");

    /**
     * the vertex format type for KMeans which output distance map
     */
    public static final String LONG_DISTANCE_MAP_NULL = "LongDistanceMapNull";
    /**
     * the vertex format type for Long Id, Double vertex value, Float edge value
     */
    public static final String LONG_DOUBLE_FLOAT = "LongDoubleFloat";
    /**
     * the vertex format type for Long Id, Two Vector vertex value, Tow Vector edge value
     */
    public static final String LONG_TWO_VECTOR_DOUBLE_TWO_VECTOR = "LongTwoVectorDoubleTwoVector";
    /**
     * the vertex format type for Long Id, Two Vector vertex value, Vector edge value
     */
    public static final String LONG_TWO_VECTOR_DOUBLE_VECTOR = "LongTwoVectorDoubleVector";
    /**
     * the vertex format type for Collaborative Filtering algorithm on property graph
     */
    public static final String PROPERTY_GRAPH_4_CF = "PropertyGraph4CF";
    /**
     * the vertex format type for Collaborative Filtering algorithm, Conjugate Gradient Descent on property graph
     */
    public static final String PROPERTY_GRAPH_4_CF_CGD = "PropertyGraph4CFCGD";
    /**
     * the vertex format type for Latent Dirichlet Alloation on property graph
     */
    public static final String PROPERTY_GRAPH_4_LDA = "PropertyGraph4LDA";
    /**
     * the id offset used by Titan
     */
    public static final long TITAN_ID_OFFSET = 4;
    /**
     * no valid property
     */
    public static final String NO_VALID_PROPERTY = "Skip this entry because no valid property for Giraph to read.";
    /**
     * invalid vertex id
     */
    public static final String INVALID_VERTEX_ID = "Vertex ID from Titan should be >0. got ";
    /**
     * invalid edge id
     */
    public static final String INVALID_EDGE_ID = "Edge ID from Titan should be >0. got ";
    /**
     * create vertex property
     */
    public static final String CREATE_VERTEX_PROPERTY = "create vertex.property in Titan ";
    /**
     * failed to open titan transaction
     */
    public static final String TITAN_TX_NOT_OPEN = "IGIRAPH ERROR: Unable to create Titan transaction! ";
    /**
     * failed to open titan transaction
     */
    public static final String OPENED_TITAN_TX = "Opened Titan transaction for graph reading.";

    /**
     * failed to open titan graph
     */
    public static final String TITAN_GRAPH_NOT_OPEN = "IGIRAPH ERROR: Unable to open Titan graph";
    /**
     * opened titan graph
     */
    public static final String OPENED_GRAPH = "opened Titan Graph";
    /**
     * closed titan graph
     */
    public static final String CLOSED_GRAPH = "closed Titan Graph";
    /**
     * input data error
     */
    public static final String INPUT_DATA_ERROR = "Error in input data: different cardinality!";
    /**
     * vertex property mismatch
     */
    public static final String VERTEX_PROPERTY_MISMATCH = "The number of output vertex property does not match! ";
    /**
     * expected size of vertex property
     */
    public static final String EXPECTED_SIZE_OF_VERTEX_PROPERTY = "The expected size of output vertex property is ";
    /**
     * current size of vertex property
     */
    public static final String REAL_SIZE_OF_VERTEX_PROPERTY = ", current size of output vertex property is ";
    /**
     * current vertex
     */
    public static final String CURRENT_VERTEX = "Current Vertex is: ";
    /**
     * No vertex read
     */
    public static final String NO_VERTEX_READ = ". Otherwise no vertex will be read from Titan.";
    /**
     * No vertex type
     */
    public static final String NO_VERTEX_TYPE = "No vertex type property specified. ";
    /**
     * No edge type
     */
    public static final String NO_EDGE_TYPE = "No edge type property specified. ";
    /**
     * No edge label
     */
    public static final String NO_EDGE_LABEL = "No input edge label specified. ";
    /**
     * No vertex property
     */
    public static final String NO_VERTEX_PROPERTY = "No vertex property list specified. ";
    /**
     * No edge property
     */
    public static final String NO_EDGE_PROPERTY = "No input edge property list specified. ";
    /**
     * ensure input format
     */
    public static final String ENSURE_INPUT_FORMAT = "Ensure your InputFormat does not require one.";
    /**
     * double check config
     */
    public static final String DOUBLE_CHECK_CONFIG = "Please double check your configuration.";
    /**
     * config titan
     */
    public static final String CONFIG_TITAN = "Please configure Titan storage ";
    /**
     * config vertex property
     */
    public static final String CONFIG_VERTEX_PROPERTY = "Please configure output vertex property list ";
    /**
     * config prefix
     */
    public static final String CONFIG_PREFIX = " by -D ";
    /**
     * ensure titan storage port
     */
    public static final String ENSURE_PORT = "Ensure you are using port ";
    /**
     * configured default
     */
    public static final String CONFIGURED_DEFAULT = " is configured as default value. ";
    /**
     * configured default
     */
    public static final String FAILED_CONNECT_HBASE_TABLE = "Failed to connect to HBase table ";
    /**
     * vertex type on the left side
     */
    public static final String LEFT_VERTEX_TYPE = "L";
    /**
     * vertex type on the right side
     */
    public static final String RIGHT_VERTEX_TYPE = "R";
    /**
     * edge type for training data
     */
    public static final String EDGE_TYPE_TRAIN = "tr";
    /**
     * edge type for validation data
     */
    public static final String EDGE_TYPE_VALIDATION = "va";
    /**
     * edge type for test data
     */
    public static final String EDGE_TYPE_TEST = "te";
    /**
     * vertex type for document
     */
    public static final String DOC_VERTEX = "d";
    /**
     * vertex type for word
     */
    public static final String WORD_VERTEX = "w";

    /**
     * prevent instantiation of utility class
     */
    private GiraphTitanConstants() {

    }
}
