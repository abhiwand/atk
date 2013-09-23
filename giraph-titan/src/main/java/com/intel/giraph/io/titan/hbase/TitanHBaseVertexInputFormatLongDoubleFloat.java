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

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.VertexInputFormat;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.titan.hbase.TitanHBaseGraphReader;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormat;
import com.intel.giraph.io.titan.conf.GiraphTitanConstants;
import org.apache.giraph.io.iterables.VertexReaderWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseKeyColumnValueStore;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

public class TitanHBaseVertexInputFormatLongDoubleFloat
		extends
		TitanHBaseVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {

	static final byte[] EDGE_STORE_FAMILY = Bytes
			.toBytes(Backend.EDGESTORE_NAME);

	private static final Logger LOG = Logger
			.getLogger(TitanHBaseVertexInputFormatLongDoubleFloat.class);

	@Override
	public void checkInputSpecs(Configuration conf) {
	}

	@Override
	public void setConf(
			ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, FloatWritable> conf) {
		conf.set(TableInputFormat.INPUT_TABLE,
				GIRAPH_TITAN_STORAGE_TABLENAME.get(conf));
		conf.set(HConstants.ZOOKEEPER_QUORUM,
				GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf));
		conf.set(HConstants.ZOOKEEPER_CLIENT_PORT,
				GIRAPH_TITAN_STORAGE_PORT.get(conf));

		Scan scanner = new Scan();
		scanner.addFamily(Backend.EDGESTORE_NAME.getBytes());

		Method converter;
		try {
			converter = TableMapReduceUtil.class.getDeclaredMethod(
					"convertScanToString", Scan.class);
			converter.setAccessible(true);
			conf.set(TableInputFormat.SCAN,
					(String) converter.invoke(null, scanner));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		super.setConf(conf);
	}

	public VertexReader<LongWritable, DoubleWritable, FloatWritable> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			RuntimeException {

		return new TitanHBaseVertexReader(split, context);

	}

	/**
	 * Uses the RecordReader to return Hbase rows
	 */
	public static class TitanHBaseVertexReader extends
			HBaseVertexReader<LongWritable, DoubleWritable, FloatWritable> {
		private TitanHBaseGraphReader graphReader;
		private Vertex vertex;
		private TaskAttemptContext context;

		public TitanHBaseVertexReader(InputSplit split,
				TaskAttemptContext context) throws IOException {
			this.context = context;
		}

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			super.initialize(inputSplit, context);
			this.graphReader = new TitanHBaseGraphReader(
					GiraphToTitanGraphFactory.generateTitanConfiguration(
							context.getConfiguration(), "giraph.titan.input"));
		}

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			if (getRecordReader().nextKeyValue()) {
				final Vertex temp = graphReader
						.readGiraphVertexLongDoubleFloat(getConf(),
								getRecordReader().getCurrentKey().copyBytes(),
								getRecordReader().getCurrentValue().getMap()
										.get(EDGE_STORE_FAMILY));
				if (null != temp) {
					vertex = temp;
					return true;
				}
			}
			return false;
		}

		@Override
		public Vertex<LongWritable, DoubleWritable, FloatWritable> getCurrentVertex()
				throws IOException, InterruptedException {
			return vertex;
		}
	}
}
