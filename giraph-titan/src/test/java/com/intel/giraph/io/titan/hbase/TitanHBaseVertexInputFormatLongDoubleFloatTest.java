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
package com.intel.giraph.io.titan.hbase;

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.STORAGE_READ_ONLY;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.AUTOTYPE;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.giraph.io.VertexReader;

import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongDoubleFloat;
import com.intel.giraph.io.titan.TitanTestGraph;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.AddedRelationsContainer;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.SimpleBufferAddedRelations;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.AttributeUtil;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.relations.StandardEdge;
import com.thinkaurelius.titan.graphdb.relations.StandardProperty;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.tinkerpop.blueprints.Direction;
//import com.tinkerpop.blueprints.Vertex;
import com.google.common.collect.Iterables;
import com.google.common.base.Charsets;

import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.Method;
import java.io.IOException;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.Assert;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;

/**
 * This test firstly load a graph to Titan/HBase, then read out the graph from
 * TitanHBaseVertexInputFormat. No special preparation needed before the test.
 */
public class TitanHBaseVertexInputFormatLongDoubleFloatTest extends
		TitanHBaseVertexInputFormatLongDoubleFloat {
	static final byte[] EDGE_STORE_FAMILY = Bytes
			.toBytes(Backend.EDGESTORE_NAME);
	public TitanTestGraph graph;
	public StandardTitanTx tx;
	protected String[] EXPECT_JSON_OUTPUT;
	private ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable> conf;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {
		GiraphConfiguration giraphConf = new GiraphConfiguration();
		giraphConf.setComputationClass(EmptyComputation.class);
		giraphConf
				.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongDoubleFloat.class);
		giraphConf
				.setVertexOutputFormatClass(JsonLongDoubleFloatDoubleVertexOutputFormat.class);
		GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
		GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
		GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
		GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
		VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "age");
		EDGE_PROPERTY_KEY_LIST.set(giraphConf, "time");
		EDGE_LABEL_LIST.set(giraphConf, "battled");
		STORAGE_READ_ONLY.set(giraphConf, "false");
		AUTOTYPE.set(giraphConf, "none");
		
	    HBaseAdmin hbaseAdmin=new HBaseAdmin(giraphConf);
	    if (hbaseAdmin.isTableAvailable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf))) {
	        hbaseAdmin.disableTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
	        hbaseAdmin.deleteTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
	    }

		conf = new ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable>(
				giraphConf);

		BaseConfiguration baseConfig = GiraphToTitanGraphFactory
				.generateTitanConfiguration(conf, "giraph.titan.input");
		GraphDatabaseConfiguration titanConfig = new GraphDatabaseConfiguration(
				baseConfig);
		graph = new TitanTestGraph(titanConfig);
		tx = graph.newTransaction(new TransactionConfig(titanConfig, false));

	}

	@Test
	public void TitanHBaseVertexInputLongDoubleFloatTest() throws Exception {

		TitanKey age = tx.makeType().name("age").unique(Direction.OUT)
				.dataType(Long.class).makePropertyKey();
		TitanKey time = tx.makeType().name("time").dataType(Integer.class)
				.unique(Direction.OUT).makePropertyKey();
		TitanLabel battled = tx.makeType().name("battled").makeEdgeLabel();

		TitanVertex n1 = tx.addVertex();
		TitanProperty p1 = n1.addProperty(age, 1000L);
		TitanVertex n2 = tx.addVertex();
		TitanProperty p2 = n2.addProperty(age, 2000L);
		TitanEdge e1 = n1.addEdge(battled, n2);
		e1.setProperty(time, 333);

		tx.commit();

		EXPECT_JSON_OUTPUT = new String[] { "[8,2000,[]]", "[4,1000,[[8,333]]]" };

		Iterable<String> results = InternalVertexRunner.run(conf,
				new String[0], new String[0]);
		Assert.assertNotNull(results);

		Iterator<String> result = results.iterator();
		int i = 0;
		while (i < EXPECT_JSON_OUTPUT.length && result.hasNext()) {
			String expectedLine = EXPECT_JSON_OUTPUT[i];
			String resultLine = (String) result.next();
			System.out.println("expected: " + expectedLine + ", got: "
					+ resultLine);
			assertEquals(resultLine, expectedLine);
			i++;
		}

	}

	@After
	public void done() throws IOException {
		close();
		System.out
				.println("***Done with TitanHBaseVertexInputLongDoubleFloatTest****");
	}

	public void close() {
		if (null != tx && tx.isOpen())
			tx.rollback();

		if (null != graph)
			graph.shutdown();
	}

	/*
	 * Test compute method that sends each edge a notification of its parents.
	 * The test set only has a 1-1 parent-to-child ratio for this unit test.
	 */
	public static class EmptyComputation
			extends
			BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
				Iterable<DoubleWritable> messages) throws IOException {
			vertex.voteToHalt();
		}
	}
}
