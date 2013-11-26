/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package com.intel.hadoop.graphbuilder.util;

import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanConfig;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.log4j.Logger;

/**
 *  Class for handling graph database connections:
 *   right now only Titan is supported
 */
public class GraphDatabaseConnector {

    private static final Logger LOG = Logger.getLogger(GraphDatabaseConnector.class);
    private static RuntimeConfig runtimeConfig = RuntimeConfig.getInstance();

    /**
     * @param graphDB                         Identifier of the target graph database,  "titan" for now
     *                                        "allegrograph" and "neo4j" are placeholders
     * @throws UnsupportedOperationException  when it cannot open the graph database, particular, if you try to
     *                                        open an unsupported graph databse
     */

    public static TitanGraph open(String graphDB, org.apache.hadoop.conf.Configuration hadoopConfig)
            throws UnsupportedOperationException, NullPointerException {

        runtimeConfig.loadConfig(hadoopConfig);

        if ("titan".equals(graphDB)) {
            BaseConfiguration configuration = new BaseConfiguration();
            configuration.setProperty("storage.backend",   TitanConfig.config.getProperty("TITAN_STORAGE_BACKEND"));
            configuration.setProperty("storage.hostname",  TitanConfig.config.getProperty("TITAN_STORAGE_HOSTNAME"));
            configuration.setProperty("storage.tablename", TitanConfig.config.getProperty("TITAN_STORAGE_TABLENAME"));
            configuration.setProperty("storage.port",      TitanConfig.config.getProperty("TITAN_STORAGE_PORT"));

            configuration.setProperty("storage.connection-timeout",
                                      TitanConfig.config.getProperty("TITAN_STORAGE_CONNECTION_TIMEOUT"));

            configuration.setProperty("ids.block-size",                "50000");
            configuration.setProperty("storage.batch-loading",         "true");

            return TitanFactory.open(configuration);

        } else if ("allegrograph".equals(graphDB)) {
            LOG.fatal("GRAPHBUILDER ERROR: Allegrograph not supported yet");
            throw new UnsupportedOperationException();
        }  else if ("neo4j".equals(graphDB)) {
            LOG.fatal("GRAPHBUILDER ERROR: neo4j not supported yet");
            throw new UnsupportedOperationException();
        }else if (null == graphDB) {
            LOG.fatal("GRAPHBUILDER ERROR: Cannot create a null graph. Please specify titan | allegrograph | neo4j");
            throw new IllegalArgumentException();
        }
        return null;
    }

    public static void checkTitanInstallation() {
        Graph             g = null;

        try {
            g = GraphDatabaseConnector.open("titan", null);
            if (g == null) {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.TITAN_ERROR,
                        "Unable to connect to Titan", LOG);
            }
        } catch (UnsupportedOperationException e) {
            LOG.fatal("GRAPHBUILDER ERROR: Unable to open graph database");
        } catch (NullPointerException e) {
            LOG.fatal("GRAPHBUILDER ERROR: attempt to open graph database using null parameter string");
        }
        finally {
            g.shutdown();
        }
    }
}