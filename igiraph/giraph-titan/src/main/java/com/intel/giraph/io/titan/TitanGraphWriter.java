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

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.TITAN_GRAPH_NOT_OPEN;

/**
 * The titan graph writer which connects Giraph to Titan
 * for writing back algorithm results
 */
public class TitanGraphWriter {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
        .getLogger(TitanGraphWriter.class);

    /**
     * Do not instantiate
     */
    private TitanGraphWriter() {
    }

    /**
     * @param context task attempt context
     * @return TitanGraph Titan graph to which Giraph write results
     */
    public static TitanGraph open(TaskAttemptContext context) throws IOException {
        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.createTitanBaseConfiguration(context.getConfiguration(),
                GIRAPH_TITAN.get(context.getConfiguration()));
        GraphDatabaseConfiguration titanConfig = new GraphDatabaseConfiguration(new CommonsConfiguration(baseConfig));

        TitanGraph graph = new StandardTitanGraph(titanConfig);

        if (null != graph) {
            return graph;
        } else {
            LOG.fatal(TITAN_GRAPH_NOT_OPEN);
            throw new RuntimeException(TITAN_GRAPH_NOT_OPEN);
        }
    }

    /**
     * @param config Immutable Giraph Configuration
     * @return TitanGraph Titan graph to which Giraph write results
     */
    public static TitanGraph open(ImmutableClassesGiraphConfiguration config) throws IOException {
        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.createTitanBaseConfiguration(config,
                GIRAPH_TITAN.get(config));
        GraphDatabaseConfiguration titanConfig = new GraphDatabaseConfiguration(new CommonsConfiguration(baseConfig));

        TitanGraph graph = new StandardTitanGraph(titanConfig);

        if (null != graph) {
            return graph;
        } else {
            LOG.fatal(TITAN_GRAPH_NOT_OPEN);
            throw new RuntimeException(TITAN_GRAPH_NOT_OPEN);
        }
    }
}


