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
package com.intel.giraph.io.titan.conf;

import org.apache.giraph.conf.StrConfOption;

/**
 * Constants used all over Giraph for configuration specific for Titan/Hbase
 * Titan/Cassandra
 */

public class GiraphTitanConstants {

    /** Titan backend type . */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_BACKEND = new StrConfOption(
            "giraph.titan.input.storage.backend", "hbase", "Titan backend - required");
    /** Titan/Hbase hostname . */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_HOSTNAME = new StrConfOption(
            "giraph.titan.input.storage.hostname", "localhost", "Titan/Hbase hostname - required");
    /** Titan/Hbase table name . */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_TABLENAME = new StrConfOption(
            "giraph.titan.input.storage.tablename", "titan", "Titan/Hbase tablename - required");
    /** port where to contact Titan/Hbase. */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_PORT = new StrConfOption(
            "giraph.titan.input.storage.port", "2181", "port where to contact Titan/hbase.");
    /** the configuration prefix to stripped for Titan */
    public static final StrConfOption GIRAPH_TITAN = new StrConfOption("giraph.titan", "giraph.titan.input",
            "Giraph/Titan prefix");
    /** Storage backend */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_READ_ONLY = new StrConfOption(
            "giraph.titan.input.storage.read-only", "false", "read only or not");
    /** backend autotype */
    public static final StrConfOption GIRAPH_TITAN_AUTOTYPE = new StrConfOption(
            "giraph.titan.input.autotype", "none", "autotype");
    /** the list of vertex properties to filter during data loading from Titan */
    public static final StrConfOption VERTEX_PROPERTY_KEY_LIST = new StrConfOption(
            "vertex.property.key.list", "age", "the vertex property keys which Giraph needs");
    /** the list of edge properties to filter during data loading from Titan */
    public static final StrConfOption EDGE_PROPERTY_KEY_LIST = new StrConfOption("edge.property.key.list",
            "battled", "the edge property keys which Giraph needs");
    /** the list of edge labels to filter during data loading from titan */
    public static final StrConfOption EDGE_LABEL_LIST = new StrConfOption("edge.label.list", "time",
            "the edge labels which Giraph needs");

    /**
     * prevent instantiation of utility class
     */
    private GiraphTitanConstants() {

    }
}
