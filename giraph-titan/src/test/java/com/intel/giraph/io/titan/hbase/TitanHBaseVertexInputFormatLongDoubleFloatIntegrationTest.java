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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongDoubleFloat;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;

import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.commons.io.FileUtils;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseKeyColumnValueStore;

import org.json.JSONException;
import org.json.JSONObject;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import java.lang.reflect.Method;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import static junit.framework.Assert.assertEquals;

public class TitanHBaseVertexInputFormatLongDoubleFloatIntegrationTest {
	/** output JSON extension */
	protected String[] EXPECT_JSON_OUTPUT;

	@Before
	@SuppressWarnings("unchecked")
	/**
	 * Before running this test, the GraphOfTheGods needs to be loaded by Titan to HBase
	 * The expected output is based on the GraphOfTheGods
	 */
	public void setUp() throws Exception {
		EXPECT_JSON_OUTPUT = new String[] { "[20,4500,[]]", "[40,0,[]]",
				"[28,45,[]]", "[48,0,[]]", "[12,0,[]]", "[36,0,[]]",
				"[8,0,[]]", "[44,0,[]]", "[16,5000,[]]",
				"[24,30,[[36,1],[40,2],[44,12]]]", "[32,4000,[]]",
				"[4,10000,[]]" };
	}

	/**
	 * When the test environment has Titan/HBase loaded the graph-of-the-god,
	 * this test should work fine. Disable by default in case the graph is not
	 * loaded
	 */
	@Ignore
	@Test
	public void TitanHBaseVertexInputLongDoubleFloatIntegrationTest()
			throws Exception {
		Iterable<String> results;
		GiraphConfiguration conf = new GiraphConfiguration();
		Iterator<String> result;

		GIRAPH_TITAN_STORAGE_BACKEND.set(conf, "hbase");
		GIRAPH_TITAN_STORAGE_HOSTNAME.set(conf, "localhost");
		GIRAPH_TITAN_STORAGE_TABLENAME.set(conf, "titan");
		GIRAPH_TITAN_STORAGE_PORT.set(conf, "2181");
		VERTEX_PROPERTY_KEY_LIST.set(conf, "age");
		EDGE_PROPERTY_KEY_LIST.set(conf, "time");
		EDGE_LABEL_LIST.set(conf, "battled");
		STORAGE_READ_ONLY.set(conf, "true");
		AUTOTYPE.set(conf, "none");

		conf.setComputationClass(EmptyComputation.class);
		conf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongDoubleFloat.class);
		conf.setVertexOutputFormatClass(JsonLongDoubleFloatDoubleVertexOutputFormat.class);

		results = InternalVertexRunner.run(conf, new String[0], new String[0]);
		Assert.assertNotNull(results);

		result = results.iterator();
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
		System.out
				.println("***Done with TitanHBaseVertexInputFormatLongDoubleFloatIntegrationTest****");
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
