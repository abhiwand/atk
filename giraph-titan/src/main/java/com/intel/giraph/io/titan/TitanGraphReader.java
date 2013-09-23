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
 */package com.intel.giraph.io.titan;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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

	/* it's only for reading a Titan graph into Hadoop. */
	private final StandardTitanTx tx;

	/** Class logger. */
	private static final Logger LOG = Logger.getLogger(TitanGraphReader.class);

	public TitanGraphReader(final Configuration configuration) {
		super(new GraphDatabaseConfiguration(configuration));
		this.tx = newTransaction(new TransactionConfig(this.getConfiguration(),
				false));
	}

	protected Vertex readGiraphVertexLongDoubleFloat(
			final ImmutableClassesGiraphConfiguration conf,
			final ByteBuffer key, Iterable<Entry> entries) {
		final GiraphVertexLoaderLongDoubleFloat loader = new GiraphVertexLoaderLongDoubleFloat(
				conf, new StaticByteBuffer(key));
		for (final Entry data : entries) {
			try {
				final GiraphVertexLoaderLongDoubleFloat.RelationFactory factory = loader
						.getFactory();
				super.edgeSerializer.readRelation(factory, data, tx);
				factory.build();
			} catch (Exception e) {
			}
		}
		return loader.getVertex();
	}


	@Override
	public void shutdown() {
		if (this.tx != null)
			this.tx.rollback();
		super.shutdown();
	}

}
