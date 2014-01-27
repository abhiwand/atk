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

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.LONG_DISTANCE_MAP_NULL;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.LONG_DOUBLE_NULL;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.LONG_LONG_NULL;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.PROPERTY_GRAPH_4_LBP;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.PROPERTY_GRAPH_4_LP;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.NO_VALID_PROPERTY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OPENED_TITAN_TX;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.PROPERTY_GRAPH_4_CF;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.PROPERTY_GRAPH_4_CF_CGD;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.PROPERTY_GRAPH_4_LDA;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.TITAN_ID_OFFSET;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.TITAN_TX_NOT_OPEN;

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
     * it's only for reading a Titan graph into Hadoop.
     */
    private final StandardTitanTx tx;

    /**
     * TitanGraphReader Constructor
     *
     * @param configuration : for StandardTitanGraph
     */
    public TitanGraphReader(final Configuration configuration) {
        super(new GraphDatabaseConfiguration(configuration));
        this.tx = newTransaction(new StandardTransactionBuilder(this.getConfiguration(), this));
        if (this.tx == null) {
            throw new RuntimeException(TITAN_TX_NOT_OPEN);
        } else {
            LOG.info(OPENED_TITAN_TX);
        }
    }

    /**
     * @param type    : input format type
     * @param conf    : Giraph configuration
     * @param key     : key value of HBase
     * @param entries : columnValues from HBase
     * @return Giraph Vertex
     */
    protected Vertex readGiraphVertex(final String type, final ImmutableClassesGiraphConfiguration conf,
                                      final ByteBuffer key, Iterable<Entry> entries) {
        StaticByteBuffer inKey = new StaticByteBuffer(key);
        long vertexId = IDHandler.getKeyID((StaticBuffer) inKey);

        if (vertexId > 0 && vertexId % TITAN_ID_OFFSET == 0) {
            switch(type) {
            case LONG_DOUBLE_NULL:
                final GiraphVertexLoaderLongDoubleNull loader1 = new GiraphVertexLoaderLongDoubleNull(conf,
                        vertexId);
                for (final Entry data : entries) {
                    try {
                        final GiraphVertexLoaderLongDoubleNull.RelationFactory factory = loader1.getFactory();
                        super.edgeSerializer.readRelation(factory, data, tx);
                        factory.build();
                    } catch (NullPointerException e) {
                        LOG.info(NO_VALID_PROPERTY);
                    }
                }
                return loader1.getVertex();


            case LONG_DISTANCE_MAP_NULL:
                final GiraphVertexLoaderLongDistanceMapNull loader2 = new GiraphVertexLoaderLongDistanceMapNull(conf,
                            vertexId);
                for (final Entry data : entries) {
                    try {
                        final GiraphVertexLoaderLongDistanceMapNull.RelationFactory factory = loader2.getFactory();
                        super.edgeSerializer.readRelation(factory, data, tx);
                        factory.build();
                    } catch (NullPointerException e) {
                        LOG.info(NO_VALID_PROPERTY);
                    }
                }
                return loader2.getVertex();

            case PROPERTY_GRAPH_4_LBP:
                final GiraphVertexLoaderPropertyGraph4LBP
                        loader3 = new GiraphVertexLoaderPropertyGraph4LBP(
                            conf, vertexId);
                for (final Entry data : entries) {
                    try {
                        final GiraphVertexLoaderPropertyGraph4LBP.RelationFactory factory = loader3
                                .getFactory();
                        super.edgeSerializer.readRelation(factory, data, tx);
                        factory.build();
                    } catch (NullPointerException e) {
                        LOG.info(NO_VALID_PROPERTY);
                    }
                }
                return loader3.getVertex();

            case PROPERTY_GRAPH_4_LP:
                final GiraphVertexLoaderPropertyGraph4LP
                        loader4 = new GiraphVertexLoaderPropertyGraph4LP(
                            conf, vertexId);
                for (final Entry data : entries) {
                    try {
                        final GiraphVertexLoaderPropertyGraph4LP.RelationFactory factory = loader4
                                .getFactory();
                        super.edgeSerializer.readRelation(factory, data, tx);
                        factory.build();
                    } catch (NullPointerException e) {
                        LOG.info(NO_VALID_PROPERTY);
                    }
                }
                return loader4.getVertex();

            case PROPERTY_GRAPH_4_CF:
                final GiraphVertexLoaderPropertyGraph4CF
                        loader5 = new GiraphVertexLoaderPropertyGraph4CF(
                            conf, vertexId);

                for (final Entry data : entries) {
                    try {
                        final GiraphVertexLoaderPropertyGraph4CF.RelationFactory factory = loader5
                                .getFactory();
                        super.edgeSerializer.readRelation(factory, data, tx);
                        factory.build();
                    } catch (NullPointerException e) {
                        LOG.info(NO_VALID_PROPERTY);
                    }
                }
                return loader5.getVertex();

            case PROPERTY_GRAPH_4_LDA:
                final GiraphVertexLoaderPropertyGraph4LDA
                        loader6 = new GiraphVertexLoaderPropertyGraph4LDA(
                            conf, vertexId);

                for (final Entry data : entries) {
                    try {
                        final GiraphVertexLoaderPropertyGraph4LDA.RelationFactory factory = loader6
                               .getFactory();
                        super.edgeSerializer.readRelation(factory, data, tx);
                        factory.build();
                    } catch (NullPointerException e) {
                        LOG.info(NO_VALID_PROPERTY);
                    }
                }
                return loader6.getVertex();

            case PROPERTY_GRAPH_4_CF_CGD:
                final GiraphVertexLoaderPropertyGraph4CFCGD
                        loader7 = new GiraphVertexLoaderPropertyGraph4CFCGD(
                            conf, vertexId);

                for (final Entry data : entries) {
                    try {
                        final GiraphVertexLoaderPropertyGraph4CFCGD.RelationFactory factory = loader7
                                .getFactory();
                        super.edgeSerializer.readRelation(factory, data, tx);
                        factory.build();
                    } catch (NullPointerException e) {
                        LOG.info(NO_VALID_PROPERTY);
                    }
                }
                return loader7.getVertex();

            case LONG_LONG_NULL:
                final GiraphVertexLoaderLongLongNull loader8 = new GiraphVertexLoaderLongLongNull(conf,
                    vertexId);
                for (final Entry data : entries) {
                    try {
                        final GiraphVertexLoaderLongLongNull.RelationFactory factory = loader8.getFactory();
                        super.edgeSerializer.readRelation(factory, data, tx);
                        factory.build();
                    } catch (NullPointerException e) {
                        LOG.info(NO_VALID_PROPERTY);
                    }
                }
                return loader8.getVertex();

            default:
                break;
            }
        }
        return null;
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
