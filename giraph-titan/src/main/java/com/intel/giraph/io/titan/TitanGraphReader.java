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

import org.apache.giraph.graph.Vertex;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.log4j.Logger;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;

/**
 * The backend agnostic Titan graph reader for pulling a graph of Titan and into
 * Giraph. Can be shared for Titan/Hbase and Titan/Cassandra
 */

public class TitanGraphReader extends StandardTitanGraph {

    /** Class logger. */
    private static final Logger LOG = Logger.getLogger(TitanGraphReader.class);

    /** it's only for reading a Titan graph into Hadoop. */
    private final StandardTitanTx tx;

    /**
     * TitanGraphReader Constructor
     *
     * @param configuration : for StandardTitanGraph
     */
    public TitanGraphReader(final Configuration configuration) {
        super(new GraphDatabaseConfiguration(configuration));
        this.tx = newTransaction(new TransactionConfig(this.getConfiguration(), false));
    }

    /**
     * readGiraphVertexLongDoubleFloat
     *
     * @param conf : Giraph configuration
     * @param key : key value of HBase
     * @param entries : columnValues from HBase
     * @return Giraph Vertex
     */
    protected Vertex readGiraphVertexLongDoubleFloat(final ImmutableClassesGiraphConfiguration conf,
            final ByteBuffer key, Iterable<Entry> entries) {
        final GiraphVertexLoaderLongDoubleFloat loader = new GiraphVertexLoaderLongDoubleFloat(conf,
                new StaticByteBuffer(key));
        for (final Entry data : entries) {
            try {
                final GiraphVertexLoaderLongDoubleFloat.RelationFactory factory = loader.getFactory();
                super.edgeSerializer.readRelation(factory, data, tx);
                factory.build();
            } catch (NullPointerException e) {
                LOG.info("Skip this entry because no valid property for Giraph to read");
                e.printStackTrace();
            }
        }
        return loader.getVertex();
    }

    /**
     * readGiraphVertexLongDoubleFloat
     *
     * @param conf : Giraph configuration
     * @param key : key value of HBase
     * @param entries : columnValues from HBase
     * @return Giraph Vertex
     */
    protected Vertex readGiraphVertexLoaderLongTwoVectorDoubleTwoVector(
            final ImmutableClassesGiraphConfiguration conf, final ByteBuffer key, Iterable<Entry> entries) {
        final GiraphVertexLoaderLongTwoVectorDoubleTwoVector
                  loader = new GiraphVertexLoaderLongTwoVectorDoubleTwoVector(
                      conf, new StaticByteBuffer(key));
        for (final Entry data : entries) {
            try {
                final GiraphVertexLoaderLongTwoVectorDoubleTwoVector.RelationFactory factory = loader
                        .getFactory();
                super.edgeSerializer.readRelation(factory, data, tx);
                factory.build();
            } catch (NullPointerException e) {
                LOG.info("Skip this entry because no valid property for Giraph to read");
            }
        }
        return loader.getVertex();
    }

    /**
     * shutdown the transaction and TitanStandardGraph
     */
    @Override
    public void shutdown() {
        if (this.tx != null) {
            this.tx.rollback();
        }
        super.shutdown();
    }

}
