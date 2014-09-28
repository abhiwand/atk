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
package com.intel.giraph.io.titan;

import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

/**
 * The backend agnostic Titan graph reader for pulling a graph of Titan and into
 * Giraph. Can be shared for Titan/Hbase and Titan/Cassandra
 */

public class TitanGraphReader extends StandardTitanGraph {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(TitanGraphReader.class);

    /**
     * TitanGraphReader Constructor
     *
     * @param configuration : for StandardTitanGraph
     */
    public TitanGraphReader(final Configuration configuration) {
        super(new GraphDatabaseConfiguration(new CommonsConfiguration(configuration)));
    }

    /**
     * shutdown the transaction and TitanStandardGraph
     */
    @Override
    public void shutdown() {
        super.shutdown();
    }

}
