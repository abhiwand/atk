/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
