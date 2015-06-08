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

import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;

/** 
 * Base class for all Titan/HBase related Giraph tests.
 * 
 * Note that these tests could still be significantly improved, this class was 
 * made by simply extracting code from existing tests into a base class. Giraph configuration
 * could be considerably simplified, the various tests that use the class could probably
 * be further simplified as well.
 *
 */
public abstract class TitanTestBase<I extends org.apache.hadoop.io.WritableComparable,
                                    V extends org.apache.hadoop.io.Writable,
                                    E extends org.apache.hadoop.io.Writable> {
    /**
     * LOG class
     */
    protected final Logger LOG = Logger.getLogger(getClass());
    public TitanTestGraph graph = null;
    public TitanTransaction tx = null;
    protected final GiraphConfiguration giraphConf = new GiraphConfiguration();

    protected GraphDatabaseConfiguration titanConfig = null;

    @Before
    public void setUp() throws Exception {
        LOG.info("*** Starting setUp ***");
        try {
            setHbaseProperties();
            open();
            configure();
        } catch (Exception e) {
            LOG.error("*** Error in SETUP ***", e);
            throw e;
        }
    }

    protected abstract void configure() throws Exception;

    protected void setHbaseProperties() {
        GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
        GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
        GIRAPH_TITAN_STORAGE_HBASE_TABLE.set(giraphConf, "titan_test");
        GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");

        GIRAPH_TITAN_STORAGE_READ_ONLY.set(giraphConf, "false");
        GIRAPH_TITAN.set(giraphConf, "giraph.titan.input");
    }

    protected void ensureTitanTableReady() throws IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        String tableName = GIRAPH_TITAN_STORAGE_HBASE_TABLE.get(giraphConf);
        if (hbaseAdmin.tableExists(tableName)) {
            //even delete an existing table needs the table is enabled before deletion
            if (hbaseAdmin.isTableDisabled(tableName)) {
                LOG.info("*** Titan table was disabled, enabling now ***");
                hbaseAdmin.enableTable(tableName);
            }

            if (hbaseAdmin.isTableAvailable(tableName)) {
                LOG.info("*** Deleting titan table ***");
                hbaseAdmin.disableTable(tableName);
                hbaseAdmin.deleteTable(tableName);
            }
        }
    }

    @After
    public void done() throws IOException {
        close();
        LOG.info("***Done with " + getClass().getName() + "****");
    }

    protected void open() throws IOException {
        LOG.info("*** Opening Titan connection ***");
        ImmutableClassesGiraphConfiguration<I, V, E> conf = new ImmutableClassesGiraphConfiguration<>(giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.createTitanBaseConfiguration(conf,
                GIRAPH_TITAN.get(giraphConf));
        titanConfig = new GraphDatabaseConfiguration(new CommonsConfiguration(baseConfig));
        ensureTitanTableReady();
        graph = new TitanTestGraph(titanConfig);
        startNewTransaction();
    }

    protected void startNewTransaction() {
        tx = graph.newTransaction();
        if (tx == null) {
            LOG.error("GIRAPH ERROR: Unable to create Titan transaction! ");
            throw new RuntimeException(
                    "execute: Failed to create Titan transaction!");
        }
        LOG.info("*** Opened Titan connection ***");
    }

    public void close() {
        if (null != tx && tx.isOpen()) {
            LOG.info("*** Rolling back transaction ***");
            tx.rollback();
        }

        if (null != graph) {
        LOG.info("*** Shutting down graph ***");
            graph.shutdown();
        }
        LOG.info("*** Closed Titan ***");
    }
}
