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
import java.util.Comparator;

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

public class TitanHBaseVertexInputFormatLongDoubleFloatMockTest extends
		TitanHBaseVertexInputFormatLongDoubleFloat {
	private RecordReader<ImmutableBytesWritable, Result> recordReader;
	private TableInputFormat tableInputFormat;
	private InputSplit inputSplit;
	private List<InputSplit> inputSplits;
	private VertexReader<LongWritable, DoubleWritable, FloatWritable> vertexReader;
	private ImmutableBytesWritable bytesWritable;
	private Result result;
	private ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable> conf;
	private TaskAttemptContext context;
	private NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fullMap;
	static final byte[] EDGE_STORE_FAMILY = Bytes
			.toBytes(Backend.EDGESTORE_NAME);
	public TitanTestGraph graph;
	public StandardTitanTx tx;
	private AtomicLong temporaryID;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {
		GiraphConfiguration giraphConf = new GiraphConfiguration();
		giraphConf.setComputationClass(DummyComputation.class);
		GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
		GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
		GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
		GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
		VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "weight");
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
		tableInputFormat = mock(TableInputFormat.class);
		recordReader = mock(RecordReader.class);
		inputSplit = mock(InputSplit.class);
		inputSplits = mock(List.class);
		bytesWritable = mock(ImmutableBytesWritable.class);
		result = mock(Result.class);
		when(recordReader.nextKeyValue()).thenReturn(true);
		when(recordReader.getCurrentKey()).thenReturn(bytesWritable);
		when(recordReader.getCurrentValue()).thenReturn(result);
		fullMap = mock(NavigableMap.class);
		when(result.getMap()).thenReturn(fullMap);
		context = mock(TaskAttemptContext.class);
		when(context.getConfiguration()).thenReturn(conf);
	}

	protected VertexReader<LongWritable, DoubleWritable, FloatWritable> createVertexReader(
			final RecordReader<ImmutableBytesWritable, Result> recordReader,
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new TitanHBaseVertexReader(split, context) {
			@Override
			protected RecordReader<ImmutableBytesWritable, Result> getRecordReader() {
				return recordReader;
			}

			@Override
			protected RecordReader<ImmutableBytesWritable, Result> createHBaseRecordReader(
					InputSplit inputSplit, TaskAttemptContext context)
					throws IOException, InterruptedException {
				return recordReader;
			}
		};
	}

	@Test
	public void TitanHBaseVertexInputLongDoubleFloatTest() throws Exception {
		TitanKey weight = tx.makeType().name("weight").unique(Direction.OUT)
				.dataType(Double.class).makePropertyKey();
		TitanKey time = tx.makeType().name("time").dataType(Integer.class)
				.unique(Direction.OUT).makePropertyKey();
		TitanLabel battled = tx.makeType().name("battled").makeEdgeLabel();

		NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = new TreeMap<byte[], NavigableMap<Long, byte[]>>(
				Bytes.BYTES_COMPARATOR);
		AddedRelationsContainer addedRelations = new SimpleBufferAddedRelations();

		TitanVertex n1 = tx.addVertex();
		TitanProperty p1 = n1.addProperty(weight, 0.0d);
		TitanVertex n2 = tx.addVertex();
		TitanProperty p2 = n2.addProperty(weight, 10.5d);
		TitanEdge e1 = n1.addEdge(battled, n2);
		e1.setProperty(time, 333);

		vertexReader = createVertexReader(recordReader, null, context);
		vertexReader
				.setConf(new ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, FloatWritable>(
						conf));
		vertexReader.initialize(inputSplit, context);

		long keyId = n1.getID();
		StaticBuffer key = IDHandler.getKey(keyId);
		byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);
		when(bytesWritable.copyBytes()).thenReturn(keyBytes);

		InternalVertex v1 = ((InternalVertex) n1).it();
		InternalVertex v2 = ((InternalVertex) n2).it();
		Object value1 = new Double(0.0d);
		Object value2 = new Double(10.5d);
		Object value3 = new Integer(333);

		// StandardProperty(long id, TitanKey type, InternalVertex vertex,
		// Object value, byte lifecycle)
		temporaryID = new AtomicLong(-1);
		StandardProperty prop = new StandardProperty(2L, SystemKey.VertexState,
				v1, (byte) 0, ElementLifeCycle.New);
		InternalRelation relation1 = (InternalRelation) prop;
		addedRelations.add(relation1);

		prop = new StandardProperty(p1.getID(), weight, v1, 0.0d,
				ElementLifeCycle.New);
		relation1 = (InternalRelation) prop;
		addedRelations.add(relation1);

		prop = new StandardProperty(4L, SystemKey.VertexState, v2, (byte) 0,
				ElementLifeCycle.New);
		relation1 = (InternalRelation) prop;
		addedRelations.add(relation1);

		prop = new StandardProperty(p2.getID(), weight, v2, 10.5d,
				ElementLifeCycle.New);
		relation1 = (InternalRelation) prop;
		addedRelations.add(relation1);

		// StandardEdge(long id, TitanLabel label, InternalVertex start,
		// InternalVertex end, byte lifecycle)
		long eid = e1.getID();
		StandardEdge edge = new StandardEdge(eid, battled, v1, v2,
				ElementLifeCycle.New);
		relation1 = (InternalRelation) edge;
		addedRelations.add(relation1);

		Collection<InternalRelation> allRelations = addedRelations.getAll();
		for (InternalRelation relation : allRelations) {
			TitanType type = relation.getType();
			for (int pos = 0; pos < relation.getLen(); pos++) {
				InternalVertex vertex = relation.getVertex(pos);
				Direction dir = EdgeDirection.fromPosition(pos);

				if (type.isUnique(dir) && ((InternalType) type).uniqueLock(dir)) {
					Entry entry = graph.writeEdge(relation, pos, tx);
					byte[] valueBytes = entry.getArrayValue();
					NavigableMap<Long, byte[]> valueMap = new TreeMap<Long, byte[]>(
							new Comparator<Long>() {
								public int compare(Long l1, Long l2) {
									return l2.compareTo(l1);
								}
							});
					valueMap.put(1379659399313L, valueBytes);
					byte[] columnBytes = entry.getArrayColumn();
					rowMap.put(columnBytes, valueMap);
				}
			}
		}

		when(fullMap.get(EDGE_STORE_FAMILY)).thenReturn(rowMap);

		assertTrue("Should have been able to read vertex",
				vertexReader.nextVertex());
		Vertex<LongWritable, DoubleWritable, FloatWritable> vertex = vertexReader
				.getCurrentVertex();

		System.out.println("vid:" + vertex.getId().get() + ", expected:"
				+ keyId);
		System.out.println("vertex value:" + vertex.getValue().get()
				+ ", expected: 0.0");

		assertEquals(keyId, vertex.getId().get());
		assertEquals(0.0d, vertex.getValue().get(), 0d);
	}

	@After
	public void done() throws IOException {
		close();
		System.out
				.println("***Done with TitanHBaseVertexInputFormatLongDoubleFloatMockTest****");
	}

	public void close() {
		if (null != tx && tx.isOpen())
			tx.rollback();

		if (null != graph)
			graph.shutdown();
	}

	public static class DummyComputation
			extends
			NoOpComputation<LongWritable, DoubleWritable, FloatWritable, Writable> {
	}

}
