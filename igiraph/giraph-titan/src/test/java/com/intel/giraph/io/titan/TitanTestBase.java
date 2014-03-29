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
package com.intel.giraph.io.titan;

import com.thinkaurelius.titan.core.TitanTransaction;
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
        GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
        GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
        GIRAPH_TITAN_STORAGE_READ_ONLY.set(giraphConf, "false");
        GIRAPH_TITAN_AUTOTYPE.set(giraphConf, "none");
        GIRAPH_TITAN.set(giraphConf, "giraph.titan.input");
    }

    protected void ensureTitanTableReady() throws IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        String tableName = GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf);
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

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
            GIRAPH_TITAN.get(giraphConf));
        titanConfig = new GraphDatabaseConfiguration(baseConfig);
        ensureTitanTableReady();
        graph = new TitanTestGraph(titanConfig);
        startNewTransaction();
    }

    protected void startNewTransaction() {
        tx = graph.newTransaction();
        if (tx == null) {
            LOG.error("IGIRAPH ERROR: Unable to create Titan transaction! ");
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
